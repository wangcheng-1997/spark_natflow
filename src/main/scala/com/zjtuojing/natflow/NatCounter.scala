package com.zjtuojing.natflow

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scalikejdbc.config.DBs
import scalikejdbc.{ConnectionPool, DB, SQL}

/**
 * ClassName IntelliJ IDEA
 * Date 2020/11/30 14:56
 */
object NatCounter {
  def main(args: Array[String]): Unit = {
    val properties: Properties = MyUtils.loadConf()

    val conf = new SparkConf()
//      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)

    val spark = SparkSession.builder().config(conf).getOrCreate()

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
           |count_min
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
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(lower*1000)

    DB.localTx { implicit session =>
      SQL("insert into nat_count_day (count_day,date) values (?,?)")
        .bind(sum1,date)
        .update()
        .apply()
      SQL("insert into nat_hbase_count_day (count_day,date) values (?,?)")
        .bind(sum2,date)
        .update()
        .apply()
    }

    DBs.closeAll()
    spark.stop()

  }
}
