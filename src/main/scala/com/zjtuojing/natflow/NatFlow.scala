package com.zjtuojing.natflow

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import com.zjtuojing.natflow.BeanClass.{NATBean, NATReportBean, SecondaryIndexBean}
import com.zjtuojing.utils.{ClickUtils, JedisPool, MyUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis
import scalikejdbc.config.DBs
import scalikejdbc.{ConnectionPool, DB, SQL}

import scala.collection.mutable.ListBuffer

/**
 * ClassName NatFlow
 * Date 2020/8/12 9:16
 */
object NatFlow {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val properties: Properties = MyUtils.loadConf()
  Class.forName("com.mysql.jdbc.Driver")

  // 指定数据库连接url，userName，password

  val url: String = properties.getProperty("mysql.url")

  val userName: String = properties.getProperty("mysql.username")

  val password: String = properties.getProperty("mysql.password")

  ConnectionPool.singleton(url, userName, password)

  DBs.setupAll()

  val tableName = "syslog"

  //     bulkload
  val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum")) //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") //设置zookeeper连接端口，默认2181
  hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

  // 初始化job，TableOutputFormat 是 org.apache.hadoop.hbase.mapred 包下的
  val jobConf = new JobConf(hbaseConf)
  jobConf.setOutputFormat(classOf[TableOutputFormat])


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      //      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "256m")
      .set("spark.kryoserializer.buffer", "64m")

    sparkConf.registerKryoClasses(
      Array(
        classOf[NATBean],
        classOf[NATReportBean],
        classOf[SecondaryIndexBean],
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
          logger.error(s"FileException $path", e)
      }
    }

    if (paths.isEmpty) {
      logger.info("paths size is  0,  empty ..... return ")
      return
    }

    val natPath = paths.mkString(",")

    val baseRDD = sc.textFile(natPath, 720)
      .map(_.trim)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    var userMaps: Map[String, String] = null

    try {
      val jedisPool = JedisPool.getJedisPool()
      val jedis = JedisPool.getJedisClient(jedisPool)
      userMaps = getUserName(jedis)
      jedis.close()
      jedisPool.destroy()
    } catch {
      case _: Exception => logger.error("redis连接异常")
    }

    // 用户信息广播变量
    val userMapsBC = sc.broadcast(userMaps)

    //    val now = new SimpleDateFormat("yyyyMMddHHmm").format(time * 1000)

    // 1 主机设备IP解析
    val hostIpRDD = baseRDD.filter(_.matches("[0-9]{2}\\.+.*"))
      .coalesce(720)
      .map(per => {
        val hostIp = per.split(" ")(0)
        ((batchTime - 300 * 1000, hostIp.substring(0, hostIp.length - 5)), 1)
      }).reduceByKey(_ + _).map(per => NATReportBean("hostIP", new Timestamp(per._1._1), per._1._2, per._2))

    // 2 日志Msg解析
    val msgRDD = baseRDD.filter(_.startsWith("Msg"))
      .coalesce(720)
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
            val msgs = per.split("\\s+")
            val information = msgs(4).split(",")
            val accesstime = msgs(1) + " " + msgs(2)
            date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(accesstime).getTime / 1000
            protocol = information(1).split("=")(1)
            sourceIp = information(2).split("=")(1)
            targetIp = information(3).split("=")(1)
            sourcePort = information(4).split("=")(1)
            targetPort = information(5).split("=")(1)
            convertedIp = information(6).split("=")(1)
            convertedPort = information(7).split("=")(1)
          }
        } catch {
          case e: Exception =>
            logger.error(per, e.getMessage)
        }

        val username = userMapsBC.value.getOrElse(sourceIp, "UnKnown")

        val rowkey = MyUtils.MD5Encode(convertedIp + "_" + convertedPort).substring(8, 24) + "_" + (Long.MaxValue - (date * 1000))

        NATBean(date, sourceIp, sourcePort, targetIp, targetPort, protocol, convertedIp, convertedPort, username, rowkey)

      }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    userMapsBC.unpersist()

    // 2.1 目标IP维度聚合
    val targetIpDetail = msgRDD.map(per => ((batchTime - 300 * 1000, per.targetIp), 1L))
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
          case _: Exception => logger.error(s"IP:{$targetIp}解析异常或无法解析")
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
    val username = msgRDD.map(per => ((batchTime - 300 * 1000, per.username), 1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
      .filter(_._1._2 != "UnKnown")

    username.coalesce(36).map(per => (per._1._2, per._2)).saveAsTextFile(s"/nat_user/${new SimpleDateFormat("yyyyMMdd/HH/mm").format(batchTime - 300 * 1000)}")

    val usernameTop = username.take(100)
      .map(per => NATReportBean("username", new Timestamp(per._1._1), per._1._2, per._2))

    // 2.6 源ip维度聚合
    val sourceIp = msgRDD.map(per => ((batchTime - 300 * 1000, per.sourceIp), 1))
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

    val userAnalyzeRDD = msgRDD
      .map(per => (per.rowkey, per)).reduceByKey((x, y) => x).map(_._2)
      //      .filter(_.username != "UnKnown")
      .coalesce(720)
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
        logger.error("写入异常")
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

      //      // TODO clickhouse实现hbase二级索引
      //      ClickUtils.clickhouseWrite(rowKeysDF, "nat_log.nat_rowKey")

    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error("写入异常")
    }

    userAnalyzeRDD
      .coalesce(360)
      .mapPartitions((per: Iterator[NATBean]) => {
        var res = List[(ImmutableBytesWritable, Put)]()
        while (per.hasNext) {
          val perline = per.next()
          val rowkey = Bytes.toBytes(perline.rowkey)
          val put = new Put(rowkey)

          put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("accesstime"), Bytes.toBytes(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(perline.accesstime * 1000)))
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
        logger.error("getUserName", e)
      //        e.printStackTrace()
    }
    maps
  }

}
