package com.zjtuojing.natflow

import scala.collection.mutable.ArrayBuffer

/**
 * ClassName BeanClass
 * Date 2019/8/13 10:07
 **/
object BeanClass {

  /**
   * 自定义类
   * ES
   */
  case class BeanForEs(
                        accesstime: Long,
                        rowkey: String,
                        name: String,
                        transIP: Long,
                        transPort: String
                      )

  /**
   * 自定义类 方便使用kryo序列化
   */
  case class BeanForNat(
                         date: String,
                         facilityIp: String,
                         sourceIP: String,
                         sourcePort: String,
                         destinationIP: String,
                         destinationPort: String,
                         changeIP: String,
                         changePort: String,
                         proto: String,
                         rowkey: String,
                         clientName: String,
                         changeIpLong: String,
                         longTime: String
                       )

  /**
   * flowmsg
   */
  case class FlowmsgData(timestamp: String, clientName: String, sourceIp: String, appName: String, bytes: Long)


  case class FlowmsgEsData(accesstime: Long, types: String, property: String, userName: String, bytes: Long)

  case class FlowmsgEsDataAppTop(accesstime: Long, types: String, property: String, userName: String, apps: ArrayBuffer[Map[String, Any]])

  case class FlowmsgEsData24Hour(accesstime: Long, types: String, property: String, userName: String, hours: ArrayBuffer[Map[String, Any]])

  // NAT日志基础解析
  case class NATBean(accesstime: Long, hostIP: String, sourceIp: String, sourcePort: String, targetIp: String, targetPort: String, protocol: String, convertedIp: String, convertedPort: String, operator: String,
                     province: String, city: String, username: String, rowkey: String)

  case class DestinationIpBean(date: Long, targetIpPort: String, var count: Long)

}
