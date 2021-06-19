package com.zjtuojing.utils

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable}

object HBaseUtils {
  object HbaseUtils {

    /**
     * HBase连接<ConnectionFactory>
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
     *   批量插入，经压测数据写入速度是原生插入方式的20-30倍
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

  }

}
