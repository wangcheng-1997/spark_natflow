package com.zjtuojing.natflow

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import com.tuojing.core.common.aes.AESUtils
import com.zjtuojing.natflow.BeanClass.{NATBean, NATReportBean, SecondaryIndexBean}
import com.zjtuojing.utils.{ClickUtils, JedisPool, MyUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis
import scalikejdbc.{ConnectionPool, DB, SQL}
import scalikejdbc.config.DBs

/**
 * ClassName NatFlow
 * Date 2020/8/12 9:16
 */
object NatFlow {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val properties: Properties = MyUtils.loadConf()
  Class.forName("com.mysql.jdbc.Driver")

  // 指定数据库连接url，userName，password

  val url = properties.getProperty("mysql.url")

  val userName = properties.getProperty("mysql.username")

  val password = properties.getProperty("mysql.password")

  ConnectionPool.singleton(url, userName, password)

  DBs.setupAll()


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

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
      call(sc, spark, time)
    })
    ssc.start()
    ssc.awaitTermination()

  }

  def call(sc: SparkContext, spark: SparkSession, batchTime: Time): Any = {
    //目录数据样本 hdfs://30.254.234.31/nat_log/20210607/09/40
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)

    val time: Long = (batchTime.milliseconds - 300000) / 1000

    val path
    //    = s"/nat_log/${new SimpleDateFormat("yyyyMMdd/HH/mm").format(time * 1000)}"
    = "/nat_log/20210607/09/40/2021_06_07_093801-6e87.log"

    var natPath: String = ""

    try {
      if (hdfs.exists(new Path(path))) {
        natPath = path
      } else {
        logger.info(s"file not exists $path")
      }
    } catch {
      case e: Exception =>
        logger.error(s"FileException $path", e)
    }

    if ("".equals(natPath)) {
      logger.info("paths size is  0,  empty ..... return ")
      return
    }

    val baseRDD = sc.textFile(natPath)
      .map(_.trim)
      .repartition(360)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val jedisPool = JedisPool.getJedisPool()
    val jedis = JedisPool.getJedisClient(jedisPool)

    // 用户信息广播变量
    val userMaps: Map[String, String] = getUserName(jedis)
    val userMapsBC = sc.broadcast(userMaps)

    val now = new SimpleDateFormat("yyyyMMddHHmm").format(time * 1000)

    //    // 1 主机设备IP解析
    //    val hostIpRDD = baseRDD.filter(_.matches("[0-9]{2}\\.+.*"))
    //      .coalesce(360)
    //      .map(per => {
    //        val hostIp = per.split(" ")(0)
    //        ((time, hostIp.substring(0, hostIp.size - 5)), 1)
    //      }).reduceByKey(_ + _).map(per => NATReportBean("hostIP", new Timestamp(per._1._1 * 1000), per._1._2, per._2))

    // 2 日志Msg解析
    val msgRDD = baseRDD.filter(_.startsWith("Msg"))
      .coalesce(360)
      .map(per => {
        var date = 0L
        var protocol = ""
        var sourceIp = ""
        var targetIp = ""
        var sourcePort = ""
        var targetPort = ""
        var convertedIp = ""
        var convertedPort = ""
        if (!per.matches("Msg: [0-9]+.*")) {
          val log = per.split(",")
          val msg = log(0).split("\\s+")
          val year = new SimpleDateFormat("yyyy").format(new Date())
          val time = new Date(s"${msg(1)} ${msg(2)} ${msg(3)} $year").getTime / 1000
          val ips = msg(8).split("[:\\->()]")
          val convert = log(1).split(" ")(3).split("[ :]")
          date = time
          protocol = ips(5)
          sourceIp = ips(1)
          targetIp = ips(3)
          sourcePort = ips(2)
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

        //  运营商信息
        var operate = "UnKnown"
        //获取省份
        var province = "UnKnown"
        //获取城市
        var city = "UnKnown"

        val maps = util.IpSearch.getRegionByIp(targetIp)
        if (!maps.isEmpty) {
          operate = maps.get("运营").toString
          province = maps.get("省份").toString
          city = maps.get("城市").toString
        }

        val username = userMapsBC.value.getOrElse(sourceIp, "UnKnown")

        val rowkey = MyUtils.MD5Encode(sourceIp + sourcePort + targetIp + targetPort + convertedIp + convertedPort).substring(8, 24) + "_" + (Long.MaxValue - (date * 1000))

        NATBean(date, sourceIp, sourcePort, targetIp, targetPort, protocol, convertedIp, convertedPort, operate, province, city, username, rowkey)


      }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    userMapsBC.unpersist()

    //    // 2.1 province维度聚合
    //    val province = msgRDD.map(per => {
    //      ((time, per.province), 1)
    //    }).reduceByKey(_ + _)
    //      .map(per => NATReportBean("province", new Timestamp(per._1._1 * 1000), per._1._2, per._2))
    //
    //    // 2.2 运营商维度聚合
    //    val operator = msgRDD.map(per => {
    //      ((time, per.operator), 1)
    //    }).reduceByKey(_ + _).map(per => NATReportBean("operator", new Timestamp(per._1._1 * 1000), per._1._2, per._2))
    //
    //    // 2.3 目标IP维度聚合
    //    val targetIp = msgRDD.map(per => ((time, per.targetIp), 1))
    //      .reduceByKey(_ + _)
    //      .sortBy(_._2)
    //      .filter(_._2 >= 100)
    //      .take(20000)
    //      .map(per => NATReportBean("targetIp", new Timestamp(per._1._1 * 1000), per._1._2, per._2))
    //
    //    // 2.4 省会维度聚合
    //    val city = msgRDD.map(per => ((time, per.city), 1))
    //      .reduceByKey(_ + _)
    //      .map(per => NATReportBean("city", new Timestamp(per._1._1 * 1000), per._1._2, per._2))
    //
    //    val provinceDF = spark.createDataFrame(province).coalesce(100)
    //    val operatorDF = spark.createDataFrame(operator).coalesce(100)
    //    val targetIpDF = spark.createDataFrame(targetIp).coalesce(100)
    //    val cityDF = spark.createDataFrame(city).coalesce(100)
    //    val hostIPDF = spark.createDataFrame(hostIpRDD).coalesce(100)

    val userAnalyzeRDD = msgRDD
      .filter(_.username != "UnKnown")
      .coalesce(360)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    //    // TODO 用户名称活跃统计
    //    msgRDD.map(per => (per.username, 1))
    //      .reduceByKey(_ + _)
    //      .coalesce(1)
    //      .saveAsTextFile(s"hdfs://nns/nat_user/${now.substring(0, 8)}/${now.substring(8, 10)}/${now.substring(10)}")
    //
    //    val rowKeys = userAnalyzeRDD.map(per => {
    //      SecondaryIndexBean(new Timestamp(per.accesstime * 1000), MyUtils.MD5Encode(per.targetIp + per.targetPort + per.convertedIp + per.convertedPort).substring(8, 24), per.rowkey)
    //    })
    //    val rowKeysDF = spark.createDataFrame(rowKeys).coalesce(100)

    val tableName = "syslog"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum")) //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") //设置zookeeper连接端口，默认2181
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    val admin = hbaseConn.getAdmin

    // 初始化job，TableOutputFormat 是 org.apache.hadoop.hbase.mapred 包下的
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val hbaseRDD = userAnalyzeRDD.map(per=>(per.rowkey,per)).reduceByKey((x,y) => x).map(_._2).sortBy(_.rowkey)
      .map(per => {

        val rowkey = Bytes.toBytes(per.rowkey)

        val immutableRowKey = new ImmutableBytesWritable(rowkey)

        val accesstime = new KeyValue(rowkey, Bytes.toBytes("nat"), Bytes.toBytes("accesstime"), Bytes.toBytes(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(per.accesstime * 1000)))
        val sourceIp = new KeyValue(rowkey, Bytes.toBytes("nat"), Bytes.toBytes("sourceIp"), Bytes.toBytes(per.sourceIp))
        val sourcePort = new KeyValue(rowkey, Bytes.toBytes("nat"), Bytes.toBytes("sourcePort"), Bytes.toBytes(per.sourcePort))
        val targetIp = new KeyValue(rowkey, Bytes.toBytes("nat"), Bytes.toBytes("targetIp"), Bytes.toBytes(per.targetIp))
        val targetPort = new KeyValue(rowkey, Bytes.toBytes("nat"), Bytes.toBytes("targetPort"), Bytes.toBytes(per.targetPort))
        val protocol = new KeyValue(rowkey, Bytes.toBytes("nat"), Bytes.toBytes("protocol"), Bytes.toBytes(per.protocol))
        val convertedIp = new KeyValue(rowkey, Bytes.toBytes("nat"), Bytes.toBytes("convertedIp"), Bytes.toBytes(per.convertedIp))
        val convertedPort = new KeyValue(rowkey, Bytes.toBytes("nat"), Bytes.toBytes("convertedPort"), Bytes.toBytes(per.convertedPort))
        val username = new KeyValue(rowkey, Bytes.toBytes("nat"), Bytes.toBytes("username"), Bytes.toBytes(per.username))

        Array(
          (immutableRowKey, accesstime),
          (immutableRowKey, sourceIp),
          (immutableRowKey, sourcePort),
          (immutableRowKey, targetIp),
          (immutableRowKey, targetPort),
          (immutableRowKey, protocol),
          (immutableRowKey, convertedIp),
          (immutableRowKey, convertedPort),
          (immutableRowKey, username)
        )
      }).flatMap(per => per)

    val table = hbaseConn.getTable(TableName.valueOf(tableName))
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)

    hbaseRDD.saveAsNewAPIHadoopFile("/test/bulkload",
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf)

    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    val regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName))
    bulkLoader.doBulkLoad(new Path("/test/bulkload"), admin, table, regionLocator)
//      .saveAsHadoopDataset(jobConf)

    //    try {
    //      //  TODO  维度聚合报表数据写入clickhouse
    //      ClickUtils.clickhouseWrite(provinceDF, "nat_log.nat_report")
    //      ClickUtils.clickhouseWrite(operatorDF, "nat_log.nat_report")
    //      ClickUtils.clickhouseWrite(targetIpDF, "nat_log.nat_report")
    //      ClickUtils.clickhouseWrite(cityDF, "nat_log.nat_report")
    //      ClickUtils.clickhouseWrite(hostIPDF, "nat_log.nat_report")
    //
    //      // TODO clickhouse实现hbase二级索引
    //      ClickUtils.clickhouseWrite(rowKeysDF, "nat_log.nat_rowKey")
    //
    //      // TODO 日志分析计数写入mysql
    //      DB.localTx { implicit session =>
    //        val nat_count = msgRDD.count()
    //        SQL("insert into nat_count (count_5min,count_sec,update_time) values (?,?,?)")
    //          .bind(nat_count, nat_count / 300, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time * 1000))
    //          .update()
    //          .apply()
    //
    //        val nat_hbase_count = userAnalyzeRDD.count()
    //        SQL("insert into nat_hbase_count (count_5min,count_sec,update_time) values (?,?,?)")
    //          .bind(nat_hbase_count, nat_hbase_count / 300, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time * 1000))
    //          .update()
    //          .apply()
    //      }
    //
    //    } catch {
    //      case e: Exception =>
    //        e.printStackTrace()
    //        logger.error("写入异常")
    //    }

    msgRDD.unpersist()


    logger.info("-----------------------------[批处理结束]")
    baseRDD.unpersist()
    jedis.close()
    jedisPool.destroy()

  }

  def getUserNameAES(jedis: Jedis): Map[String, String] = {
    var maps = Map[String, String]()
    try {
      maps = jedis.hgetAll("nat:radius")
        .values().toArray
        .map(json => {
          val jobj = JSON.parseObject(AESUtils.decryptData("tj!@#123#@!tj&!$", json.toString))
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
