package com.zjtuojing.natflow

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import com.zjtuojing.natflow.BeanClass.{NATBean, NATReportBean}
import com.zjtuojing.utils.{ClickUtils, JedisPoolSentine, MyUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis
import scalikejdbc.config.DBs
import scalikejdbc.{ConnectionPool, DB, SQL}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * ClassName NatFlowDelay
 * Date 2020/8/12 9:16
 */
object BulkLoadNatFlow {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val properties: Properties = MyUtils.loadConf()
  Class.forName("com.mysql.jdbc.Driver")

  // 指定数据库连接url，userName，password

  val url: String = properties.getProperty("mysql.url")

  val userName: String = properties.getProperty("mysql.username")

  val password: String = properties.getProperty("mysql.password")

  ConnectionPool.singleton(url, userName, password)

  DBs.setupAll()

  var jedis: Jedis = null
  try {
    val jedisPool = JedisPoolSentine.getJedisPool()
    jedis = JedisPoolSentine.getJedisClient(jedisPool)
  } catch {
    case e: Exception => logger.warn("redis连接异常")
    case e: java.lang.Exception => logger.warn("redis连接异常")
  }

  val tableName = "syslog"

  //     bulkload
  val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "30.254.234.31,30.254.234.33,30.254.234.35,30.254.234.36,30.254.234.38")
  hbaseConf.set("zookeeper.znode.parent", "/hbase")
  hbaseConf.set("hbase.client.retries.number", "3")
  hbaseConf.set("hbase.rpc.timeout", "200000")
  hbaseConf.set("hbase.client.operation.timeout", "300000")
  hbaseConf.set("hbase.client.scanner.timeout.period", "100000")




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
    var time = 0L
    //多取前一个文件， 部分记录在前一个文件中
    //目录数据样本 hdfs://30.250.60.7/dns_log/2019/12/25/1621_1577262060
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    for (w <- 10 * 30 until 0 by -60) {
      time = (batchTime - w * 1000L) / 1000
      val path = s"/nat_log/${new SimpleDateFormat("yyyyMMdd/HH/mm").format(time * 1000)}"

      try {
        if (hdfs.exists(new Path(path))) {
          //        if (w >= 580) {
          paths += path
        } else {
          logger.info(s"file not exists $path")
        }
      } catch {
        case e: Exception =>
          logger.warn(s"FileException $path", e)
      }
    }

    if (paths.isEmpty) {
      logger.info("paths size is  0,  empty ..... return ")
      return
    }

    val natPath = paths.mkString(",")

    val baseRDD = sc.textFile(natPath)
      .map(_.trim)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val partitions = baseRDD.getNumPartitions

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
      .coalesce(partitions)
      .map(per => {
        val hostIpList = per.split(" ")(0).split("\\.")
        val hostIp = hostIpList(0) + "." + hostIpList(1) + "." + hostIpList(2) + "." + hostIpList(3)
        ((batchTime - 300 * 1000, hostIp,Random.nextInt(100)), 1)
      })
      .reduceByKey(_ + _)
      .map(per => ((per._1._1, per._1._2), per._2))
      .reduceByKey(_ + _)
      .map(per => NATReportBean("hostIP", new Timestamp(per._1._1), per._1._2, per._2))

    // 2 日志Msg解析
    val msgRDD = baseRDD.filter(_.startsWith("Msg"))
      .coalesce(partitions)
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
              date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(accesstime).getTime / 1000
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
    val targetIpDetail = msgRDD.map(per => ((batchTime - 300 * 1000, per.targetIp, Random.nextInt(100)), 1L))
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
    val username = msgRDD.map(per => ((batchTime - 300 * 1000, per.username, Random.nextInt(100)), 1))
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
    val sourceIp = msgRDD.map(per => ((batchTime - 300 * 1000, per.sourceIp, Random.nextInt(100)), 1))
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

    val userAnalyzeRDD = msgRDD.coalesce(partitions)
      .map(per => (per.rowkey, per))
      .reduceByKey((x, y) => x)
      .map(_._2)
      //      .filter(_.username != "UnKnown")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    try {
      // TODO 日志分析计数写入mysql
      DB.localTx { implicit session =>
        val nat_count = msgRDD.count()
        SQL("insert into nat_count (count_5min,count_sec,update_time) values (?,?,?)")
          .bind(nat_count, nat_count / 300, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(batchTime - 300 * 1000))
          .update()
          .apply()

        val nat_hbase_count = userAnalyzeRDD.count()
        SQL("insert into nat_hbase_count (count_5min,count_sec,update_time) values (?,?,?)")
          .bind(nat_hbase_count, nat_hbase_count / 300, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(batchTime - 300 * 1000))
          .update()
          .apply()
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.warn("写入异常")
    }

    //    val rowKeys = userAnalyzeRDD.map(per => {
    //      SecondaryIndexBean(new Timestamp(per.accesstime * 1000), MyUtils.MD5Encode(per.targetIp + per.targetPort + per.convertedIp + per.convertedPort).substring(8, 24), per.rowkey)
    //    })
    //    val rowKeysDF = spark.createDataFrame(rowKeys).coalesce(100)

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

      val writeRdd = userAnalyzeRDD
        .coalesce(partitions)
        .map((perline: NATBean) => {
            val rowKey = perline.rowkey
            List(
//              (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("nat"), Bytes.toBytes("accesstime"), Bytes.toBytes(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(perline.accesstime * 1000)))),
//              (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("nat"), Bytes.toBytes("sourceIp"), Bytes.toBytes(perline.sourceIp))),
//              (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("nat"), Bytes.toBytes("sourcePort"), Bytes.toBytes(perline.sourcePort))),
//              (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("nat"), Bytes.toBytes("targetIp"), Bytes.toBytes(perline.targetIp))),
//              (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("nat"), Bytes.toBytes("targetPort"), Bytes.toBytes(perline.targetPort))),
//              (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("nat"), Bytes.toBytes("protocol"), Bytes.toBytes(perline.protocol))),
//              (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("nat"), Bytes.toBytes("convertedIp"), Bytes.toBytes(perline.convertedIp))),
//              (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("nat"), Bytes.toBytes("convertedPort"), Bytes.toBytes(perline.convertedPort))),
//              (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("nat"), Bytes.toBytes("username"), Bytes.toBytes(perline.username)))
            (rowKey,"accesstime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(perline.accesstime * 1000)),
            (rowKey,"sourceIp", perline.sourceIp),
            (rowKey,"sourcePort", perline.sourcePort),
            (rowKey,"targetIp", perline.targetIp),
            (rowKey,"targetPort", perline.targetPort),
            (rowKey,"protocol", perline.protocol),
            (rowKey,"convertedIp", perline.convertedIp),
            (rowKey,"convertedPort", perline.convertedPort),
            (rowKey,"username", perline.username)
            )
        }).flatMap(per => per)
        .sortBy(per => per._1+"nat"+per._2)
        .map(per => {
          val rowKey = per._1
          (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("nat"), Bytes.toBytes(per._2), Bytes.toBytes(per._3)))
        })


      //生成的HFile的临时保存路径
      val stagingFolder = "hdfs://30.254.234.33:8020/hfile_tmp"
      //将日志保存到指定目录
      writeRdd.saveAsNewAPIHadoopFile(stagingFolder,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        hbaseConf)
      //此处运行完成之后,在stagingFolder会有我们生成的Hfile文件

      //开始即那个HFile导入到Hbase,此处都是hbase的api操作
      val load = new LoadIncrementalHFiles(hbaseConf)
      //hbase的表名
      val tableName = "syslog"
      //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
      val conn = ConnectionFactory.createConnection(hbaseConf)
      //根据表名获取表
      val table: Table = conn.getTable(TableName.valueOf(tableName))
      try {
        //创建一个hadoop的mapreduce的job
        val job = Job.getInstance(hbaseConf)
        //设置job名称
        job.setJobName("DumpFile")
        //此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
        job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
        //输出文件的内容KeyValue
        job.setMapOutputValueClass(classOf[KeyValue])
        //配置HFileOutputFormat2的信息
        HFileOutputFormat2.configureIncrementalLoadMap(job, table)
        //开始导入
        val start = System.currentTimeMillis()
        load.doBulkLoad(new Path(stagingFolder), table.asInstanceOf[HTable])
        val end = System.currentTimeMillis()
        println("用时：" + (end - start) + "毫秒！")
        hdfs.delete(new Path("/hfile_tmp"))
      } finally {
        table.close()
        conn.close()
      }

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

}
