package com.zjtuojing.natflow

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import com.zjtuojing.natflow.BeanClass.{NATBean, NATReportBean}
import com.zjtuojing.utils.HBaseUtils.hexSplit
import com.zjtuojing.utils.{ClickUtils, JedisPoolSentine, MyUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{Jedis, JedisPool, JedisSentinelPool}
import scalikejdbc.config.DBs
import scalikejdbc.{ConnectionPool, DB, SQL}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * ClassName NatFlowDelay
 * Date 2020/8/12 9:16
 */
object DelayNatFlow {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val properties: Properties = MyUtils.loadConf()
  Class.forName("com.mysql.jdbc.Driver")

  // 指定数据库连接url，userName，password

  val url: String = properties.getProperty("mysql.url")

  val userName: String = properties.getProperty("mysql.username")

  val password: String = properties.getProperty("mysql.password")

  ConnectionPool.singleton(url, userName, password)

  DBs.setupAll()

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sparkConf.registerKryoClasses(
      Array(
        classOf[NATBean],
        classOf[NATReportBean],
        classOf[ImmutableBytesWritable]
      )
    )

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext
    //HDFS HA
    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sc.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))

    // 用textFileStream的API定时读取一个空的文件 实现实时框架调度离线程序
    val ssc = new StreamingContext(sc, Seconds(300))
    val stream = ssc.textFileStream("home/DontDelete")
    stream.foreachRDD((rdd, time) => {
      logger.info(s"task start ..... batch time:{$time}", rdd.count())
      //      获取当前时间
      call(sc, spark, time.milliseconds)
    })
    ssc.start()
    ssc.awaitTermination()

  }

  def call(sc: SparkContext, spark: SparkSession, batchTime: Long): Any = {
    val paths = new ListBuffer[String]()

    //多取前一个文件， 部分记录在前一个文件中
    //目录数据样本 hdfs://30.250.60.7/dns_log/2019/12/25/1621_1577262060
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)

    val date = new SimpleDateFormat("yyyyMMdd").format(batchTime - 300000)

    //目录数据样本 hdfs://30.250.60.7/dns_log/2019/12/25/1621_1577262060
    val files = hdfs.listFiles(new Path(s"/nat_log/$date/"), true)

    var files_size = 0L

    val limit = if (getDataSpeedLimit(spark).length == 1) {
      getDataSpeedLimit(spark)(0)
    }
    else {
      40
    }

    val block_paths = new ListBuffer[(String, String)]
    var jedisPool: JedisSentinelPool = null
    var jedis: Jedis = null
    try {
      jedisPool = JedisPoolSentine.getJedisPool()
      jedis = JedisPoolSentine.getJedisClient(jedisPool)
    } catch {
      case e: Exception => logger.warn("redis连接异常")
      case e: java.lang.Exception => logger.warn("redis连接异常")
    }

    try {
      val files_redis_path = jedis.hkeys("nat:nat_files_delay").toArray()
      val files_old_path = sc.parallelize(files_redis_path)
        .map(per => (per.toString, 1))
        .sortByKey()
        .map(per => new Path(per._1))
        .collect()

      val files_old = hdfs.listStatus(files_old_path).toList.iterator
      while (files_old.hasNext) {
        val file = files_old.next()
        val filePath = file.getPath
        val file_size = file.getLen / 1024 / 1024
        if (files_size / 1024 <= limit) {
          paths.append(filePath.toString)
          files_size = files_size + file_size
        } else {
          block_paths.append((filePath.toString, file_size.toString))
        }
      }
      jedis.del("nat:nat_files_delay")
      if (block_paths.length != 0) {
        block_paths.toArray.foreach(per => {
          jedis.hset("nat:nat_files_delay", per._1, per._2)
        })
      }
    } catch {
      case e: Exception => logger.error("无延迟数据", e)
    }

    while (files.hasNext) {
      val file = files.next()
      val accessTime = file.getModificationTime
      if (accessTime >= batchTime - 300000 && accessTime < batchTime) {
        if (files_size / 1024 <= limit.toInt) {
          val filePath = file.getPath
          val file_size = file.getLen / 1024 / 1024
          paths.append(filePath.toString)
          files_size = files_size + file_size
        } else {
          val filePath = file.getPath
          val file_size = file.getLen / 1024 / 1024
          jedis.hset("nat:nat_files_delay", filePath.toString, file_size.toString)
        }
      }
    }

    logger.info("读取路径:" + paths.mkString(","))

    val natPath = paths.mkString(",").replaceAll("hdfs://nns", "")

    val baseRDD = sc.textFile(natPath)
      .map(_.trim)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    var userMaps: Map[String, String] = null

    if (jedis != null) {
      try {
        userMaps = getUserName(jedis)
      } catch {
        case _: Exception => logger.warn("redis连接异常")
      }
    }

    // 用户信息广播变量
    val userMapsBC = sc.broadcast(userMaps)

    // 1 主机设备IP解析
    val hostIpRDD = baseRDD.filter(_.matches("[0-9]{2}\\.+.*"))
      .coalesce(800)
      .map(per => {
        val hostIpList = per.split(" ")(0).split("\\.")
        val hostIp = hostIpList(0) + "." + hostIpList(1) + "." + hostIpList(2) + "." + hostIpList(3)
        ((batchTime - 300000, hostIp, Random.nextInt(100)), 1)
      })
      .reduceByKey(_ + _)
      .map(per => ((per._1._1, per._1._2), per._2))
      .reduceByKey(_ + _)
      .map(per => NATReportBean("hostIP", new Timestamp(per._1._1), per._1._2, per._2))

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val batchformat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    // 2 日志Msg解析
    val msgRDD = baseRDD.filter(_.startsWith("Msg"))
      .repartition(800)
      .map(per => {
        var date = 0L
        var protocol = ""
        var sourceIp = "0.0.0.0"
        var targetIp = "0.0.0.0"
        var sourcePort = ""
        var targetPort = ""
        var convertedIp = "0.0.0.0"
        var convertedPort = ""
        try {
          if (!per.matches("Msg: [0-9]+.*")) {
            val log = per.split(",")
            val msg = log(0).split("\\s+")
            val year = new SimpleDateFormat("yyyy").format(new Date())
            val time = new Date(s"${msg(1)} ${msg(2)} ${msg(3)} $year").getTime / 1000
            val ips = msg(8).split("[:\\->()]")
            val convert = log(1).split(" ")(3).split("[ :]")
            date = time
            protocol = ips(5)
            sourceIp = ips(0)
            targetIp = ips(3)
            sourcePort = ips(1)
            targetPort = ips(4)
            convertedIp = convert(0)
            convertedPort = convert(1)
          } else {
            try {
              val msgs = per.split("\\s+")
              val accesstime = msgs(1) + " " + msgs(2)
              date = format.parse(accesstime).getTime / 1000
              val information = msgs(4).split(",")
              protocol = information(1).split("=")(1)
              if (msgs.size == 5) {
                sourceIp = information(2).split("=")(1)
                targetIp = information(3).split("=")(1)
                sourcePort = information(4).split("=")(1)
                targetPort = information(5).split("=")(1)
                convertedIp = information(6).split("=")(1)
                convertedPort = information(7).split("=")(1)
              }
            } catch {
              case e: Exception => logger.warn(per)
            }
          }
        } catch {
          case e: Exception =>
            logger.warn(per, e.getMessage)
        }

        val username = if (userMapsBC != null) userMapsBC.value.getOrElse(sourceIp, "UnKnown")
        else "UnKnown"

        val rowkey = MyUtils.MD5Encode(convertedIp + "_" + convertedPort).substring(8, 24) + "_" + (Long.MaxValue - (date * 1000))

        NATBean(date, sourceIp, sourcePort, targetIp, targetPort, protocol, convertedIp, convertedPort, username, rowkey)

      }).filter(_.sourceIp != "0.0.0.0").persist(StorageLevel.MEMORY_AND_DISK_SER)

    userMapsBC.unpersist()

    // 2.1 目标IP维度聚合
    val targetIpDetail = msgRDD
      .map(per => {
        val msg_time = batchformat.format(per.accesstime * 1000)
        val time_min = msg_time.substring(14, 16).toInt
        val accesstime = batchformat.parse(msg_time.replaceAll(":[0-5][0-9]", s":${time_min / 5 * 5}")).getTime
        ((accesstime, per.targetIp, Random.nextInt(100)), 1L)
      })
      .reduceByKey(_ + _)
      .map(per => ((per._1._1, per._1._2), per._2))
      .reduceByKey(_ + _)
      .map((per: ((Long, String), Long)) => {
        val accesstime = per._1._1
        val targetIp = per._1._2
        //  运营商信息
        var operate = "UnKnown"
        //获取省份
        var province = "UnKnown"
        //获取城市
        var city = "UnKnown"
        var maps = util.IpSearch.getRegionByIp("0.0.0.0")
        try {
          maps = util.IpSearch.getRegionByIp(targetIp)
        } catch {
          case _: Exception => logger.warn(s"IP:{$targetIp}解析异常或无法解析")
        }
        if (!maps.isEmpty) {
          operate = maps.get("运营").toString
          province = maps.get("省份").toString
          city = maps.get("城市").toString
        }
        (accesstime, targetIp, province, operate, city, per._2)
      }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val targetIp = targetIpDetail
      .sortBy(-_._6)
      .take(100)
      .map(per => NATReportBean("targetIp", new Timestamp(per._1), per._2, per._6))

    // 2.2 province维度聚合
    val province = targetIpDetail.map(per => ((per._1, per._3), per._6))
      .reduceByKey(_ + _)
      .map(per => NATReportBean("province", new Timestamp(per._1._1), per._1._2, per._2))

    // 2.3 运营商维度聚合
    val operator = targetIpDetail.map(per => ((per._1, per._4), per._6))
      .reduceByKey(_ + _)
      .map(per => NATReportBean("operator", new Timestamp(per._1._1), per._1._2, per._2))

    // 2.4 省会维度聚合
    val city = targetIpDetail.map(per => ((per._1, per._5), per._6))
      .reduceByKey(_ + _)
      .map(per => NATReportBean("city", new Timestamp(per._1._1), per._1._2, per._2))

    // 2.5 宽带账号维度聚合
    val username = msgRDD
      .map(per => {
        val msg_time = batchformat.format(per.accesstime * 1000)
        val time_min = msg_time.substring(14, 16).toInt
        val accesstime = batchformat.parse(msg_time.replaceAll(":[0-5][0-9]", s":${time_min / 5 * 5}")).getTime
        ((accesstime, per.username, Random.nextInt(100)), 1L)
      })
      .reduceByKey(_ + _)
      .map(per => ((per._1._1, per._1._2), per._2))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
      .filter(_._1._2 != "UnKnown")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    username
      .map(per => (per._1._2, per._2))
      .saveAsTextFile(s"/nat_user/${new SimpleDateFormat("yyyyMMdd/HH/mm").format(batchTime - 300 * 1000)}")

    val usernameTop = username.take(100)
      .map(per => NATReportBean("username", new Timestamp(per._1._1), per._1._2, per._2))

    // 2.6 源ip维度聚合
    val sourceIp = msgRDD
      .map(per => {
        val msg_time = batchformat.format(per.accesstime * 1000)
        val time_min = msg_time.substring(14, 16).toInt
        val accesstime = batchformat.parse(msg_time.replaceAll(":[0-5][0-9]", s":${time_min / 5 * 5}")).getTime
        ((accesstime, per.sourceIp, Random.nextInt(100)), 1L)
      })
      .reduceByKey(_ + _)
      .map(per => ((per._1._1, per._1._2), per._2))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
      .take(100)
      .map(per => NATReportBean("sourceIp", new Timestamp(per._1._1), per._1._2, per._2))

    val targetIpDF = spark.createDataFrame(targetIp).coalesce(100)
    val provinceDF = spark.createDataFrame(province).coalesce(100)
    val operatorDF = spark.createDataFrame(operator).coalesce(100)
    val cityDF = spark.createDataFrame(city).coalesce(100)
    val hostIPDF = spark.createDataFrame(hostIpRDD).coalesce(100)
    val usernameDF = spark.createDataFrame(usernameTop).coalesce(100)
    val sourceIpDF = spark.createDataFrame(sourceIp).coalesce(100)

    val userAnalyzeRDD = msgRDD.coalesce(800)
      .map(per => (per.rowkey, per))
      .reduceByKey((x, y) => x)
      .map(_._2)
      //      .filter(_.username != "UnKnown")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    try {
      // TODO 日志分析计数写入mysql
      DB.localTx { implicit session =>
        val nat_count = msgRDD
          .map(per => {
            val msg_time = batchformat.format(per.accesstime * 1000)
            val time_min = msg_time.substring(14, 16).toInt
            val accesstime = batchformat.parse(msg_time.replaceAll(":[0-5][0-9]", s":${time_min / 5 * 5}")).getTime
            (accesstime, 1L)
          })
          .reduceByKey(_ + _)
          .filter(per => per._1 >= batchTime - 3 * 3600 * 1000)
          .collect()

        //        val nat_count_lost = msgRDD
        //          .map(per => {
        //            val msg_time = batchformat.format(per.accesstime * 1000)
        //            val time_min = msg_time.substring(14, 16).toInt
        //            val accesstime = batchformat.parse(msg_time.replaceAll(":[0-5][0-9]", s":${time_min / 5 * 5}")).getTime
        //            (accesstime, 1L)
        //          })
        //          .reduceByKey(_ + _)
        //          .filter(per => per._1 <= batchTime - 3 * 3600 * 1000)
        //          .collect().toList
        //        logger.info(nat_count_lost.toString())

        nat_count.foreach(per => {
          SQL("insert into nat_count (count_5min,count_sec,update_time,accesstime) values (?,?,?,?)")
            .bind(per._2, per._2 / 300, format.format(per._1), format.format(batchTime - 300000))
            .update()
            .apply()
        })


        val nat_hbase_count = userAnalyzeRDD
          .map(per => {
            val msg_time = batchformat.format(per.accesstime * 1000)
            val time_min = msg_time.substring(14, 16).toInt
            val accesstime = batchformat.parse(msg_time.replaceAll(":[0-5][0-9]", s":${time_min / 5 * 5}")).getTime
            (accesstime, 1L)
          })
          .reduceByKey(_ + _)
          .filter(per => per._1 >= batchTime - 3 * 3600 * 1000)
          .collect()

        nat_hbase_count.foreach(per => {
          SQL("insert into nat_hbase_count (count_5min,count_sec,update_time,accesstime) values (?,?,?,?)")
            .bind(per._2, per._2 / 300, format.format(per._1), format.format(batchTime - 300000))
            .update()
            .apply()
        })
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.warn("写入异常")
    }

    try {
      //  TODO  维度聚合报表数据写入clickhouse
      ClickUtils.clickhouseWrite(provinceDF, "nat_log.nat_report")
      ClickUtils.clickhouseWrite(operatorDF, "nat_log.nat_report")
      ClickUtils.clickhouseWrite(targetIpDF, "nat_log.nat_report")
      ClickUtils.clickhouseWrite(cityDF, "nat_log.nat_report")
      ClickUtils.clickhouseWrite(hostIPDF, "nat_log.nat_report")
      ClickUtils.clickhouseWrite(usernameDF, "nat_log.nat_report")
      ClickUtils.clickhouseWrite(sourceIpDF, "nat_log.nat_report")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.warn("写入异常")
    }

    try {
      val tableName = "syslog" + date

      //     bulkload
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum")) //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") //设置zookeeper连接端口，默认2181
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

      val connection = ConnectionFactory.createConnection(hbaseConf)
      val admin = connection.getAdmin
      if (!admin.tableExists(TableName.valueOf(tableName))) {
        val descriptor = admin.getTableDescriptor(TableName.valueOf("syslog"))
        //分区的数量
        val regionNum = 20
        val splits = hexSplit(regionNum)
        descriptor.setName(TableName.valueOf(tableName))
        admin.createTable(descriptor, splits)
      }

      // 初始化job，TableOutputFormat 是 org.apache.hadoop.hbase.mapred 包下的
      val jobConf = new JobConf(hbaseConf)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      userAnalyzeRDD
        .coalesce(800)
        .mapPartitions((per: Iterator[NATBean]) => {
          var res = List[(ImmutableBytesWritable, Put)]()
          while (per.hasNext) {
            val perline = per.next()
            val rowkey = Bytes.toBytes(perline.rowkey)
            val put = new Put(rowkey)

            put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("accesstime"), Bytes.toBytes(format.format(perline.accesstime * 1000)))
            put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("sourceIp"), Bytes.toBytes(perline.sourceIp))
            put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("sourcePort"), Bytes.toBytes(perline.sourcePort))
            put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("targetIp"), Bytes.toBytes(perline.targetIp))
            put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("targetPort"), Bytes.toBytes(perline.targetPort))
            put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("protocol"), Bytes.toBytes(perline.protocol))
            put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("convertedIp"), Bytes.toBytes(perline.convertedIp))
            put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("convertedPort"), Bytes.toBytes(perline.convertedPort))
            put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("username"), Bytes.toBytes(perline.username))
            res.::=(new ImmutableBytesWritable, put)
          }
          res.iterator
        })
        .saveAsHadoopDataset(jobConf)

      connection.close()

    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.warn("写入异常")
    }

    username.unpersist()
    userAnalyzeRDD.unpersist()
    targetIpDetail.unpersist()
    msgRDD.unpersist()
    baseRDD.unpersist()
    if (jedis != null && jedisPool != null) {
      jedis.close()
      jedisPool.destroy()
    }

    logger.info("-----------------------------[批处理结束]")
  }

  def getUserName(jedis: Jedis): Map[String, String] = {
    var maps = Map[String, String]()
    try {
      maps = jedis.hgetAll("nat:radius")
        .values().toArray
        .map(json => {
          val jobj = JSON.parseObject(json.toString)
          (jobj.getString("ip"), jobj.getString("account"))
        }).toMap
    } catch {
      case e: Exception =>
        maps = maps
        logger.warn("getUserName", e)
      //        e.printStackTrace()
    }
    maps
  }

  def getDataSpeedLimit(spark: SparkSession): Array[Int] = {
    //导入隐式转换
    import spark.implicits._
    val limit = Utils.ReadMysql(spark, "sys_cluster_set")
      .map(row => {
        val limit = row.getAs[Int]("performance")
        limit
      }).rdd.collect()
    limit
  }

}
