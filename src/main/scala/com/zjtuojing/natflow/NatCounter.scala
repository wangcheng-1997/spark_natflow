package com.zjtuojing.natflow

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.zjtuojing.utils.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import scalikejdbc.config.DBs
import scalikejdbc.{ConnectionPool, DB, SQL}

/**
 * ClassName IntelliJ IDEA
 * Date 2020/11/30 14:56
 */
object NatCounter {


  /**
   * ClassName NatCounter
   * Date 2020/6/2 17:28
   */
  val month = new SimpleDateFormat("yyyyMM").format(new Date)

  def main(args: Array[String]): Unit = {

    val properties = MyUtils.loadConf()

    if (args.length != 1) {
      println(
        """
          |参数错误：
          |<taskType hh,dd>
          |""".stripMargin)
      sys.exit()
    }

    getFlowCountDD(properties)


    val Array(task) = args

    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ESAggregate")
      //      .setMaster("local[*]")


    //采用kryo序列化库
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册
    //      .set("mapping.date.rich", "false")
    //      .set("max.docs.per.partition","100")
    //      .set("spark.es.scroll.size","10000")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sc.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))

    getFlowCountDD(properties)

//    getNATAnalyzeDD(sc)
//    task match {
//      case "hh" => getNATAnalyze(sc)
//      case "dd" =>
//
//        getFlowCountDD(properties)
//      case _ => println("NAT日志分析跳过执行...")
//    }

  }

  def getFlowCountDD(properties: Properties) = {
    val lower: Long = MyUtils.getTaskTime._3._2
    val upper: Long = MyUtils.getTaskTime._3._1

    Class.forName("com.mysql.jdbc.Driver")

    // 指定数据库连接url，userName，password

    val url = properties.getProperty("mysql.url")

    val userName = properties.getProperty("mysql.username")

    val password = properties.getProperty("mysql.password")

    ConnectionPool.singleton(url, userName, password)

    DBs.setupAll()

    val nat_count = DB.readOnly { implicit session =>
      SQL(
        s"""
           |select
           |count_5min
           |from
           |nat_count
           |where
           |UNIX_TIMESTAMP(update_time)<$upper
           |and UNIX_TIMESTAMP(update_time)>=$lower""".stripMargin)
        .map(rs => {
          val count_min = rs.long(1)
          count_min
        })
        .list()
        .apply()
    }
    val nat_hbase_count = DB.readOnly { implicit session =>
      SQL(
        s"""
           |select
           |count_5min
           |from
           |nat_hbase_count
           |where
           |UNIX_TIMESTAMP(update_time)<$upper
           |and UNIX_TIMESTAMP(update_time)>=$lower""".stripMargin)
        .map(rs => {
          val count_min = rs.long(1)
          count_min
        })
        .list()
        .apply()
    }
    val sum1 = nat_count.sum
    val sum2 = nat_hbase_count.sum
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(lower * 1000)

    DB.localTx { implicit session =>
      SQL("insert into nat_count_day (count_day,date) values (?,?)")
        .bind(sum1, date)
        .update()
        .apply()
      SQL("insert into nat_hbase_count_day (count_day,date) values (?,?)")
        .bind(sum2, date)
        .update()
        .apply()
    }

    DBs.closeAll()

  }

  private def getNATAnalyze(sc: SparkContext): Unit = {

    case class EsNatAnalyze(accesstime: Long, types: String, data: String, count: Long, key: String)

    val query1 = s"""{\"query\":{\"bool\":{\"must\":[{\"term\":{ \"accesstime\": \"${MyUtils.getTaskTime._3._1 * 1000L}\"}}]}}}"""

    val maps: Map[String, Long] = sc.esRDD(s"bigdata_nat_report", query1)
      .values
      .map(per => {
        val key = per.getOrElse("key", "unknown").toString
        val value = per.getOrElse("count", 0).toString.toDouble.toLong
        (key, value)
      })
      .collect.toMap[String, Long]

    val broadMaps: Broadcast[Map[String, Long]] = sc.broadcast(maps)

    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val date = dateFormat.format(new Date().getTime)
    val lower = MyUtils.getTaskTime._1._2
    val upper = MyUtils.getTaskTime._1._1
    val query2 = s"""{\"query\":{\"bool\":{\"must\":[{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""
    val valueRdd: RDD[EsNatAnalyze] = sc.esRDD(s"bigdata_nat_flow_${date}", query2)
      .values
      .map(per => {
        //        val date: Long = dateFormat.parse(yesterday).getTime / 1000
        val date: Long = MyUtils.getTaskTime._3._1
        val data: String = per.getOrElse("data", "").toString
        val count = per.getOrElse("count", "0").toString.toDouble.toLong
        val types: String = per.getOrElse("types", "").toString
        val key = date + "_" + types + "_" + data
        (key, count)
      })
      .distinct()
      .map(per => (per._1, per._2))
      .reduceByKey(_ + _)
      .map(per => {
        val mapsValue: Map[String, Long] = broadMaps.value
        val accesstime: Long = per._1.split("_")(0).toLong * 1000
        val types: String = per._1.split("_")(1)
        val data: String = per._1.split("_")(2)
        if (mapsValue.nonEmpty && mapsValue.contains(per._1)) {
          EsNatAnalyze(accesstime, types, data, per._2 + mapsValue(per._1), per._1)
        } else {
          EsNatAnalyze(accesstime, types, data, per._2, per._1)
        }
      })

    EsSpark.saveToEs(valueRdd, "bigdata_nat_report/nat", Map("es.mapping.id" -> "key"))

  }

  private def getNATAnalyzeDD(sc: SparkContext): Unit = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val date: String = dateFormat.format(System.currentTimeMillis())
    val lower = MyUtils.getTaskTime._3._2

    val destinationIp = sc.textFile(s"hdfs://nns/NATIp/$date*/*/part*")
      .map(perline => {
        val strings: Array[String] = perline.split("[,()]")
        val accesstime: Long = dateFormat.parse(dateFormat.format(strings(1).toLong * 1000)).getTime
        val data: String = strings(2)
        val count = strings(3).toLong
        (("destinationIp", accesstime, data), count)
      })
      .filter(_._1._2 == lower * 1000L)
      .reduceByKey(_ + _)
      .map(per => Map("accesstime" -> per._1._2, "types" -> per._1._1, "data" -> per._1._3, "count" -> per._2))

    EsSpark.saveToEs(destinationIp, "bigdata_nat_report/nat")
  }


}

