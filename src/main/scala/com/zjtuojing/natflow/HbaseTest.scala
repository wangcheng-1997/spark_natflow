package com.zjtuojing.natflow

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import redis.clients.jedis.Jedis


/**
 * ClassName IntelliJ IDEA
 * Date 2020/10/16 15:26
 */
object HbaseTest {
  def main(args: Array[String]): Unit = {
    val properties = MyUtils.loadConf()

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val datetime = dateFormat.format(System.currentTimeMillis())

    //    // 加载配置信息
    Class.forName("com.mysql.jdbc.Driver")
    //    // 指定数据库连接url，userName，password
    val url3 = "jdbc:mysql://30.250.60.35:3306/nat_log?characterEncoding=utf-8&useSSL=false"
    val userName1 = properties.getProperty("mysql.username")
    val password1 = properties.getProperty("mysql.password")
    val natlog_connection = DriverManager.getConnection(url3, userName1, password1)

    val statement2: Statement = natlog_connection.createStatement()

    val before_value: ResultSet = statement2.executeQuery(s"select * from nat_count where update_time='${datetime.substring(0, 15) + datetime.substring(15, 16).toInt / 5 * 5 + ":00"}'")
    var hasRows = false
    while (before_value.next) {
      hasRows = true
    }

    if (!hasRows) {
      val last_value1: ResultSet = statement2.executeQuery("select count_min from nat_count order by seq desc limit 1")
      val longs1 = new collection.mutable.ListBuffer[Long]
      while (last_value1.next()) longs1 += last_value1.getLong(1)
      statement2.executeUpdate(s"insert into nat_count (count_min,count_sec,update_time) values ('${longs1(0)}','${longs1(0) / 300}','${datetime.substring(0, 15) + datetime.substring(15, 16).toInt / 5 * 5 + ":00"}')")
      val last_value2: ResultSet = statement2.executeQuery("select count_min from nat_count order by seq desc limit 1")
      val longs2 = new collection.mutable.ListBuffer[Long]
      while (last_value2.next()) longs2 += last_value2.getLong(1)
      statement2.executeUpdate(s"insert into nat_hbase_count (count_5min,count_sec,update_time) values ('${longs2(0)}','${longs2(0) / 300}','${datetime.substring(0, 15) + datetime.substring(15, 16).toInt / 5 * 5 + ":00"}')")
    }

  }

}
