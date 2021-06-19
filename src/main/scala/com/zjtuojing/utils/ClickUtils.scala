package com.zjtuojing.utils

import java.sql.{SQLFeatureNotSupportedException, Timestamp}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scalikejdbc.{ConnectionPool, DB, SQL}
import scalikejdbc.config.DBs
import util.IpSearch


object ClickUtils {
  val prop = MyUtils.loadConf()
  val url = prop.getProperty("clickhouse.url")
  val user = prop.getProperty("clickhouse.user")
  val password = prop.getProperty("clickhouse.password")

  val ch_prop = new Properties()
  ch_prop.put("driver", "com.github.housepower.jdbc.ClickHouseDriver")
  //  prop.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  ch_prop.put("user", user)
  ch_prop.put("password", password)


  def clickhouseWrite(df: DataFrame, table: String): Unit = {
    try {
      df.write.mode(saveMode = "append")
        .option("batchsize", "100000")
        .option("isolationLevel", "NONE") // 设置事务
        .option("numPartitions", "1") // 设置并发
        .jdbc(url, table, ch_prop)
    } catch {
      case e: SQLFeatureNotSupportedException =>
        println("catch and ignore!")
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[6]")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    case class nat_detail(ip: String, province: String, city: String, operator: String)
    val frame = spark.read.jdbc(url, "nat_log.nat_report", ch_prop)
    val tuples = frame.rdd.map(per => (per.getString(0), per.getTimestamp(1), per.getString(2), per.getLong(3)))
      .filter(_._1 == "targetIp")
      .filter(_._2 == new Timestamp(1623897300000L))
      .sortBy(-_._4)
      .take(500)
      .map(per => per._3)
      //      .foreach(println)
      .map((per: String) => {
        val maps = IpSearch.getRegionByIp(per)
        var operator = "未知"
        var province = "未知"
        var city = "未知"
        if (!maps.isEmpty) {
          operator = maps.get("运营").toString
          province = maps.get("省份").toString
          city = maps.get("城市").toString
        }
        nat_detail(per, province, city, operator)
      })

    Class.forName("com.mysql.jdbc.Driver")

    // 指定数据库连接url，userName，password

    val mysql_url = prop.getProperty("mysql.url")

    val userName = prop.getProperty("mysql.username")

    val password = prop.getProperty("mysql.password")

    ConnectionPool.singleton(mysql_url, userName, password)

    DBs.setupAll()

    try {
      // TODO 日志分析计数写入mysql
      DB.localTx { implicit session =>
        for (o <- tuples) {
          SQL("insert into nat_ip_detail (ip,province,city,operator) values (?,?,?,?)")
            .bind(o.ip, o.province, o.city, o.operator)
            .update()
            .apply()
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }


  }
}
