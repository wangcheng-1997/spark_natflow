package com.zjtuojing.natflow

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.tuojing.core.common.aes.AESUtils
import com.zjtuojing.natflow.BeanClass.{NATBean, NATReportBean, SecondaryIndexBean}
import com.zjtuojing.utils.{ClickUtils, MyUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

/**
 * ClassName NatFlow
 * Date 2020/8/12 9:16
 */
object NatFlowHDFS {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val properties: Properties = MyUtils.loadConf()

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sparkConf.registerKryoClasses(
      Array(
        classOf[NATBean],
        classOf[NATReportBean],
        classOf[SecondaryIndexBean]
      )
    )

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    // 用textFileStream的API定时读取一个空的文件 实现实时框架调度离线程序
    val ssc = new StreamingContext(sc, Seconds(300))
    val stream = ssc.textFileStream("home/DontDelete")
    stream.foreachRDD((rdd, time) => {
      logger.info("task start ..... batch time:{time}", rdd.count())
      //      获取当前时间
      call(sc, spark, time)
    })
    ssc.start()
    ssc.awaitTermination()

  }

  def call(sc: SparkContext, spark: SparkSession, batchTime: Time): Any = {
    val baseRDD = sc.textFile("D:\\QQFile\\425845843\\FileRecv\\syslog")
      .map(_.trim)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val time: Long = (batchTime.milliseconds - 300000) / 1000
    val now = new SimpleDateFormat("yyyyMMddHHmm").format(time * 1000)

    // 1 主机设备IP解析
    val hostIpRDD = baseRDD.filter(_.matches("[0-9]{2}\\.+.*"))
      .coalesce(180)
      .map(per => {
        val hostIp = per.split(" ")(0)
        ((time, hostIp), 1)
      }).reduceByKey(_ + _).map(per => NATReportBean("hostIP", new Timestamp(per._1._1 * 1000), per._1._2, per._2))

    // 2 日志Msg解析
    val msgRDD = baseRDD.filter(_.startsWith("Msg"))
      .coalesce(180)
      .map(per => {
        val msgs = per.split("\\s+")
        val information = msgs(4).split(",")
        val accesstime = msgs(1) + " " + msgs(2).substring(0, 3) + msgs(2).substring(3, 5).toInt / 5 * 5 + ":00"
        val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(accesstime).getTime / 1000
        val hostIp = msgs(3)
        val protocol = information(1).split("=")(1)
        val sourceIp = information(2).split("=")(1)
        val targetIp = information(3).split("=")(1)
        val sourcePort = information(4).split("=")(1)
        val targetPort = information(5).split("=")(1)
        val convertedIp = information(6).split("=")(1)
        val convertedPort = information(7).split("=")(1)

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

        val username = "unknown"
        val rowkey = MyUtils.MD5Encode(sourceIp + sourcePort + targetIp + targetPort + convertedIp + convertedPort).substring(8, 24) + "_" + (date * 1000)

        NATBean(date, hostIp, sourceIp, sourcePort, targetIp, targetPort, protocol, convertedIp, convertedPort, operate, province, city, username, rowkey)
      }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 2.1 province维度聚合
    val province = msgRDD.map(per => {
      ((time, per.province), 1)
    }).reduceByKey(_ + _)
      .map(per => NATReportBean("province", new Timestamp(per._1._1 * 1000), per._1._2, per._2))

    // 2.2 运营商维度聚合
    val operator = msgRDD.map(per => {
      ((time, per.operator), 1)
    }).reduceByKey(_ + _).map(per => NATReportBean("operator", new Timestamp(per._1._1 * 1000), per._1._2, per._2))

    // 2.3 目标IP维度聚合
    val targetIp = msgRDD.map(per => ((time, per.targetIp), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .filter(_._2 >= 100)
      .take(70000)
      .map(per => NATReportBean("targetIp", new Timestamp(per._1._1 * 1000), per._1._2, per._2))

    // 2.4 省会维度聚合
    val city = msgRDD.map(per => ((time, per.city), 1))
      .reduceByKey(_ + _)
      .map(per => NATReportBean("city", new Timestamp(per._1._1 * 1000), per._1._2, per._2))

    val provinceDF = spark.createDataFrame(province)
    val operatorDF = spark.createDataFrame(operator)
    val targetIpDF = spark.createDataFrame(targetIp)
    val cityDF = spark.createDataFrame(city)
    val hostIPDF = spark.createDataFrame(hostIpRDD)

    //  TODO  维度聚合报表数据写入clickhouse
    ClickUtils.clickhouseWrite(provinceDF, "nat_log.nat_report")
    ClickUtils.clickhouseWrite(operatorDF, "nat_log.nat_report")
    ClickUtils.clickhouseWrite(targetIpDF, "nat_log.nat_report")
    ClickUtils.clickhouseWrite(cityDF, "nat_log.nat_report")
    ClickUtils.clickhouseWrite(hostIPDF, "nat_log.nat_report")

    val userAnalyzeRDD = msgRDD.filter(_.username != "UnKnown")
      .coalesce(180)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // TODO 用户名称活跃统计
    msgRDD.map(per => (per.username, 1))
      .reduceByKey(_ + _)
      .coalesce(1)
      .saveAsTextFile(s"hdfs://nns/nat_user/${now.substring(0, 8)}/${now.substring(8, 10)}/${now.substring(10)}")

    //ES实现hbase二级索引
    val rowKeys = userAnalyzeRDD.map(per => {
      SecondaryIndexBean(new Timestamp(per.accesstime * 1000), per.sourceIp, per.sourcePort, MyUtils.MD5Encode(per.targetIp + per.targetPort + per.convertedIp + per.convertedPort), per.rowkey)
    })
    val rowKeysDF = spark.createDataFrame(rowKeys)

    try {
      ClickUtils.clickhouseWrite(rowKeysDF, "nat_log.nat_rowKey")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error("写入异常")
    }

    val tableName = "syslog"
    val conf = HBaseConfiguration.create()
    val jobConf = new JobConf(conf)
    jobConf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum"))
    jobConf.set("zookeeper.znode.parent", "/hbase")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    userAnalyzeRDD.map(per => {

      val rowkey = Bytes.toBytes(per.rowkey)
      val put = new Put(rowkey)

      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("accesstime"), Bytes.toBytes(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(per.accesstime * 1000)))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("hostIP"), Bytes.toBytes(per.hostIP))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("sourceIp"), Bytes.toBytes(per.sourceIp))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("sourcePort"), Bytes.toBytes(per.sourcePort))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("targetIp"), Bytes.toBytes(per.targetIp))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("targetPort"), Bytes.toBytes(per.targetPort))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("protocol"), Bytes.toBytes(per.protocol))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("convertedIp"), Bytes.toBytes(per.convertedIp))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("convertedPort"), Bytes.toBytes(per.convertedPort))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("username"), Bytes.toBytes(per.username))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
    msgRDD.unpersist()

    logger.info("-----------------------------[批处理结束]")
    baseRDD.unpersist()

  }

  def getUserName(jedis: Jedis): Map[String, String] = {
    var maps = Map[String, String]()
    try {
      maps = jedis.hgetAll("ONLINEUSERS:USER_OBJECT")
        .values().toArray
        .map(json => {
          val jobj = JSON.parseObject(AESUtils.decryptData("tj!@#123#@!tj&!$", json.toString))
          (jobj.getString("ip"), jobj.getString("user"))
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
