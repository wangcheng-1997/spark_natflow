package com.zjtuojing.natflow

import java.sql.Timestamp

import scala.collection.mutable.ArrayBuffer

/**
 * ClassName BeanClass
 * Date 2019/8/13 10:07
 **/
object BeanClass {

  // NAT日志基础解析
  case class NATBean(accesstime: Long, hostIP: String, sourceIp: String, sourcePort: String, targetIp: String, targetPort: String, protocol: String, convertedIp: String, convertedPort: String, operator: String,
                     province: String, city: String, username: String, rowkey: String)

  case class NATReportBean(types: String, accesstime: Timestamp, data: String, count: Int)

  case class SecondaryIndexBean(accesstime: Timestamp, sourceIp: String, sourcePort: String, partKey: String, rowkey: String)

}
