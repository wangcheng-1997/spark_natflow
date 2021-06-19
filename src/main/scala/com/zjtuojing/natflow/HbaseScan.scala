package com.zjtuojing.natflow

import java.util.Properties

import com.zjtuojing.utils.MyUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.filter.{CompareFilter, PageFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object HbaseScan {

  val properties: Properties = MyUtils.loadConf()

  val con: Configuration = HBaseConfiguration.create()
  // 设置zookeeperjiqun
  con.set("hbase.zookeeper.quorum", "30.254.234.31:2181,30.254.234.33:2181,30.254.234.35:2181,30.254.234.36:2181,30.254.234.38:2181")
  // 建立连接
  val conf: Connection = ConnectionFactory.createConnection(con)
  // 获取管理员
  val admin: Admin = conf.getAdmin
  // 创建表名称对象，通过这个对象才可以操作表
  val tableName: TableName = TableName.valueOf("syslog")
  // 通过连接对象获取表，操作表中数据
  val table: Table = conf.getTable(tableName)

  val logger = LoggerFactory.getLogger(this.getClass)


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
              .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sc.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))
    select()
    sc.stop()
    spark.close()
  }

  def select(): Unit = {
    val scan = new Scan()
    scan.setTimeRange(1623816000000L, 1623819600000L)

//    val filter = new SingleColumnValueFilter(
//      Bytes.toBytes("nat"),
//      Bytes.toBytes("sourceIp"),
//      CompareFilter.CompareOp.EQUAL, Bytes.toBytes("30.130.74.28"))

    val filter = new PageFilter(1000)
    scan.setFilter(filter)
    val l1 = System.currentTimeMillis()
    logger.info(s"开始查询:$l1")
    val resultScanner: ResultScanner = table.getScanner(scan)

    var result: Result = resultScanner.next()
    var counter = 0
    while (result != null && counter <= 10) {
      val cells: Array[Cell] = result.rawCells()
      cells.foreach(x => logger.info(
        Bytes.toString(CellUtil.cloneRow(x)) + "  " + // 获取row
          Bytes.toString(CellUtil.cloneFamily(x)) + "  " + // 获取列族
          Bytes.toString(CellUtil.cloneQualifier(x)) + "  " + // 获取列名
          Bytes.toString(CellUtil.cloneValue(x)))) // 获取值
      // 循环结束条件
      counter = counter + 1
      result = resultScanner.next()
    }

    val l2 = System.currentTimeMillis()
    logger.info(s"查询结束:$l2")
    logger.info(s"查询用时:${l2 - l1}")

  }
}
