package com.zjtuojing.natflow

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import com.zjtuojing.utils.MyUtils
import kafka.common.TopicAndPartition
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
 * ClassName MySQLUtils
 * Date 2019/7/15 15:07
 **/
object Utils {

  val properties = MyUtils.loadConf()


  def apply() = {
    DBs.setup()
  }


  /**
   * 根据groupId查询偏移量数据，并将结果封装成Map[TopicAndPartition, Long]
   */
  def getOffsetsBy(groupId: String): Map[TopicAndPartition, Long] = {

    DB.readOnly { implicit session =>
      SQL("select * from kafka_offset where groupId = ?")
        .bind(groupId)
        .map(rs => (TopicAndPartition(rs.string("topic"), rs.int("partitionNum")), rs.long("offsets")))
        .list().apply()
    }.toMap

  }


  /**
   * 时间戳转换为date
   */
  def timestamp2Date(pattern: String, timestamp: Long): String = {
    val time: String = new SimpleDateFormat(pattern).format(timestamp)
    time
  }


  /**
   * 读取mysql数据
   */
  def ReadMysql(spark: SparkSession, tableName: String): DataFrame = {

    val df = spark.read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("dbtable", tableName)
      .load()

    df
  }


  /**
   * 写入MySQL
   */
  def Write2Mysql(df: DataFrame, tableName: String) = {
    df.write.mode(SaveMode.Overwrite)
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("dbtable", tableName)
  }

}
