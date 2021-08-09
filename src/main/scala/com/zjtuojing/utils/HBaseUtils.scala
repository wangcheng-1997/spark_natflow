package com.zjtuojing.utils

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, HTable, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}

object HBaseUtils {


  /**
   * HBase连接<ConnectionFactory>
   *
   * @param zkList
   * @param port
   * @return
   */
  def getHBaseConn(zkList: String, port: String): Connection = {
    //     bulkload
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "30.254.234.31,30.254.234.33,30.254.234.35,30.254.234.36,30.254.234.38")
    hbaseConf.set("zookeeper.znode.parent", "/hbase")
    hbaseConf.set("hbase.client.retries.number", "3")
    hbaseConf.set("hbase.rpc.timeout", "200000")
    hbaseConf.set("hbase.client.operation.timeout", "300000")
    hbaseConf.set("hbase.client.scanner.timeout.period", "100000")
    val connection = ConnectionFactory.createConnection(hbaseConf)
    connection
  }

  /**
   * HBase连接<HTable>
   * 批量插入，经压测数据写入速度是原生插入方式的20-30倍
   *
   * @param zkList
   * @param port
   * @return
   */
  def getHTable(zkList: String, port: String, tableName: String): HTable = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkList)
    conf.set("hbase.zookeeper.property.clientPort", port)
    val table = new HTable(conf, TableName.valueOf(tableName))
    table
  }


  def main(args: Array[String]): Unit = {
    val connection = getHBaseConn("30.254.234.31,30.254.234.33,30.254.234.35,30.254.234.36,30.254.234.38", "2181")
    //    val admin = connection.getAdmin
    //    val descriptor = admin.getTableDescriptor(TableName.valueOf("syslog"))
    //    val string = new SimpleDateFormat("yyyyMMdd").format(new Date())
    //    //分区的数量
    //    val regionNum = 100
    //    val splits = hexSplit(regionNum)
    //    descriptor.setName(TableName.valueOf("syslog"+string))
    //    admin.createTable(descriptor,splits)
    val table = connection.getTable(TableName.valueOf("syslog20210806"))
    val scan = new Scan()
    //      scan.setTimeRange(1623816000000L, 1623819600000L)

    val filter = new SingleColumnValueFilter(
      Bytes.toBytes("nat"),
      Bytes.toBytes("convertedIp"),
      CompareFilter.CompareOp.EQUAL, Bytes.toBytes("113.214.201.110"))

    //      val filter = new PageFilter(1000)
    scan.setFilter(filter)
    val l1 = System.currentTimeMillis()
    println(s"开始查询:$l1")
    val resultScanner: ResultScanner = table.getScanner(scan)

    var result: Result = resultScanner.next()
    var counter = 0
    while (result != null && counter <= 10) {
      val cells: Array[Cell] = result.rawCells()
      cells.foreach(x => println(
        Bytes.toString(CellUtil.cloneRow(x)) + "  " + // 获取row
          Bytes.toString(CellUtil.cloneFamily(x)) + "  " + // 获取列族
          Bytes.toString(CellUtil.cloneQualifier(x)) + "  " + // 获取列名
          Bytes.toString(CellUtil.cloneValue(x)))) // 获取值
      // 循环结束条件
      counter = counter + 1
      result = resultScanner.next()
    }

    val l2 = System.currentTimeMillis()
    println(s"查询结束:$l2")
    println(s"查询用时:${l2 - l1}")

    connection.close()
  }

  import java.io.IOException

  import org.apache.hadoop.hbase.util.RegionSplitter

  @throws[IOException]
  def hexSplit(regionNum: Int): Array[Array[Byte]] = {
    val algo = new RegionSplitter.HexStringSplit
    val splits = algo.split(regionNum)
    splits
  }

}
