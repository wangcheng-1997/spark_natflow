package com.zjtuojing.natflow

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.zjtuojing.utils.{ClickUtils, MyUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scalikejdbc.config.DBs
import scalikejdbc.{ConnectionPool, DB, SQL}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object LivenessRpt {

  case class region(var rid1: String = "", var rid2: String = "", var rid3: String = "", var rid4: String = "")

  def main(args: Array[String]): Unit = {
    val properties = MyUtils.loadConf()
    val datetime = MyUtils.getTaskTime._3._2
    val date = new SimpleDateFormat("yyyyMMdd").format(datetime * 1000)

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      //      .setMaster("local[*]")
      .set("spark.speculation", "false")
      .set("spark.locality.wait", "10")
      .set("spark.storage.memoryFraction", "0.3")

    //采用kryo序列化库
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册
    conf.registerKryoClasses(
      Array(
        classOf[Array[String]],
        classOf[Map[String, Any]]
      ))
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    //HADOOP HA
    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sc.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))

    val value = sc.textFile(s"hdfs://nns/nat_user/$date/*/*")
      .coalesce(108)
      .map(per => {
        val users = per
        val str = per.substring(1, users.length - 1).split(",")
        if (str.size == 2) (str(0), str(1).toInt)
        else if (str.size == 3) (str(0) + "," + str(1), str(2).toInt)
        else (str(0) + "," + str(1) + "," + str(2), str(3).toInt)
      })
      .reduceByKey(_ + _)
      .map(per => {
        (per._1, per._2)
      })

    val online_users = value.collect().toMap
    val online_users_broad = sc.broadcast(online_users)

    Class.forName("com.mysql.jdbc.Driver")

    // 指定数据库连接url，userName，password
    val userName = properties.getProperty("mysql.username")

    val password = properties.getProperty("mysql.password")

    val prop = new Properties()
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("user", userName)
    prop.put("password", password)

    val arr = ArrayBuffer[Int]()
    for (i <- 0 until 100) {
      arr.append(i)
    }
    val predicates = arr.map(i => {
      s"SHA1(id)%100 = $i"
    }).toArray

    val users_regions = spark.read
      .jdbc("jdbc:mysql://30.254.234.21:3306/nat_log?characterEncoding=utf8&useSSL=false", "sys_wasu_user", predicates, prop)
      .select("username", "rid1", "rid2", "rid3", "rid4")
      .rdd
      .map(per => {
        val username = per.get(0).toString
        val rid1 = per.get(1).toString
        val rid2 = per.get(2).toString
        val rid3 = per.get(3).toString
        val rid4 = per.get(4).toString
        val resolver = online_users_broad.value.getOrElse(username, 0)
        (username, resolver, new Timestamp(datetime * 1000), rid1, rid2, rid3, rid4)
      })

    import spark.implicits._

    val users_regionsArray = users_regions.collect().toList
    val users_regionsBuffer = new ListBuffer[List[(String, Int, Timestamp, String, String, String, String)]]
    val users_regionsBufferSize = users_regionsArray.length / 500000

    for (i <- 0 to users_regionsBufferSize) {
      if ((i + 1) * 500000 < users_regionsArray.length)
        users_regionsBuffer += users_regionsArray.slice(i * 500000, (i + 1) * 500000)
      else users_regionsBuffer += users_regionsArray.slice(i * 500000, users_regionsArray.length - 1)
    }

    for (j <- users_regionsBuffer.indices) {
      val frame = users_regionsBuffer(j).toDF("username", "resolver", "accesstime", "rid1", "rid2", "rid3", "rid4")
      ClickUtils.clickhouseWrite(frame, "nat_log.nat_liveness")
      Thread.sleep(5000)
    }

    getFlowCountDD(properties)
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

}
