package com.zjtuojing.natflow

import java.sql.Timestamp

import scala.collection.mutable.ArrayBuffer

/**
 * ClassName BeanClass
 * Date 2019/8/13 10:07
 **/
object BeanClass {

  def main(args: Array[String]): Unit = {
    val value = "30.254.215.25.2000"
    println(value)
    val str = value.substring(0, value.size - 5)
    println(str)
  }
  // NAT日志基础解析
  case class NATBean(accesstime: Long, sourceIp: String, sourcePort: String, targetIp: String, targetPort: String, protocol: String, convertedIp: String, convertedPort: String, username: String, rowkey: String)

  case class NATReportBean(types: String, accesstime: Timestamp, data: String, count: Long)

  case class SecondaryIndexBean(accesstime: Timestamp, partKey: String, rowkey: String)

}
