package com.zjtuojing.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable}

object HBaseUtils {


  /**
   * HBase连接<ConnectionFactory>
   *
   * @param zkList
   * @param port
   * @return
   */
  def getHBaseConn(zkList: String, port: String): Connection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkList)
    conf.set("hbase.zookeeper.property.clientPort", port)
    val connection = ConnectionFactory.createConnection(conf)
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
    val admin = connection.getAdmin
    val descriptor = admin.getTableDescriptor(TableName.valueOf("syslog"))
    val string = new SimpleDateFormat("yyyyMMdd").format(new Date())
    //分区的数量
    val regionNum = 100
    val splits = hexSplit(regionNum)
    descriptor.setName(TableName.valueOf("syslog"+string))
    admin.createTable(descriptor,splits)
    connection.close()
  }

  import org.apache.hadoop.hbase.util.RegionSplitter
  import java.io.IOException
  import java.math.BigInteger

  @throws[IOException]
  def hexSplit(regionNum: Int): Array[Array[Byte]] = {
    val algo = new RegionSplitter.HexStringSplit
    val splits = algo.split(regionNum)
    splits
  }

}
