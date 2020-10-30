package com.zjtuojing.natflow

import java.io.FileInputStream
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

object MyUtils {

  /**
    * xx.xx.xx.xx转换为Long类型
    *
    * @param ip
    * @return
    */
  def ipToLong(ip: String): Long = {
    val arr = ip.split("\\.")
    var ip2long = 0L
    if (arr.length == 4) {
      var i = 0
      while ( {
        i < 4
      }) {
        ip2long = ip2long << 8 | arr(i).toInt

        {
          i += 1
          i
        }
      }
    }
    ip2long
  }


  /**
    * get APP
    *
    * @param ip
    * @param appBro
    * @return
    */
  def getAppName(ip: Long, appBro: Array[(Long, Long, String)]): String = {
    var appName = "其他"
    appBro.map(bro => {
      val start = bro._1
      val end = bro._2
      if (start <= ip && end > ip) {
        appName = bro._3
      }
      appName
    })
    appName
  }


  def getHourAndByte(dataList: List[(String, String)]): String = {

    val array: Array[String] = Array("00=0", "01=0", "02=0", "03=0", "04=0", "05=0", "06=0",
      "07=0", "08=0", "09=0", "10=0", "11=0", "12=0", "13=0", "14=0",
      "15=0", "16=0", "17=0", "18=0", "19=0", "20=0", "21=0", "22=0",
      "23=0")

    for(o <- 0 to array.length - 1){
      val files = array(o).split("=")
      val hour = files(0)
      var byte = files(1)
      dataList.foreach(tuple => {
        if(hour == tuple._1){
          byte = tuple._2
        }
      })
      array(o) = hour+"="+byte
    }
    array.toList.mkString(",")
  }

  def getTaskTime = {
    val df1 = new SimpleDateFormat("yyyy-MM-dd HH")
    val df2 = new SimpleDateFormat("yyyy-MM-dd")
    val df3 = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val now = new Date()
    val time = df1.format(now.getTime)
    val time1 = df1.parse(time).getTime / 1000

    val now_hour = time.substring(11, 13).toInt
    val time2 = df1.parse(time.replaceAll("\\s+\\d{2}", s" ${now_hour / 6 * 6}")).getTime / 1000

    val time3 = df2.parse(df2.format(now.getTime)).getTime / 1000

    val timem = df3.format(now)
    val now_min = timem.substring(14, 16).toInt
    val time4 = df3.parse(timem.replaceAll(":[0-5][0-9]", s":${now_min / 5 * 5}")).getTime / 1000

    val hour: Long = time1 - 3600
    val sixHour: Long = time2 - 21600
    val date: Long = time3 - 86400
    val minute: Long = time4 - 300

    ((time1, hour), (time2, sixHour), (time3, date), (time4, minute))
  }

  def getTaskTime(msgtime: Long) = {
    val df1 = new SimpleDateFormat("yyyy-MM-dd HH")
    val df2 = new SimpleDateFormat("yyyy-MM-dd")
    val df3 = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val time = df1.format(msgtime * 1000)
    val time1: Long = df1.parse(time).getTime / 1000

    val now_hour = time.substring(11, 13).toInt
    val time2 = df1.parse(time.replaceAll("\\s+\\d{2}", s" ${now_hour / 6 * 6}")).getTime / 1000

    val time3 = df2.parse(df2.format(msgtime * 1000)).getTime / 1000

    val timem = df3.format(msgtime * 1000)
    val now_min = timem.substring(14, 16).toInt
    val time4 = df3.parse(timem.replaceAll(":[0-5][0-9]", s":${now_min / 5 * 5}")).getTime / 1000

    val hour: Long = time1 - 3600
    val sixHour: Long = time2 - 21600
    val date: Long = time3 - 86400
    val minute: Long = time4 - 300

    ((time1, hour), (time2, sixHour), (time3, date), (time4, minute))
  }

  def MD5Encode(input: String): String = {

    // 指定MD5加密算法
    val md5: MessageDigest = MessageDigest.getInstance("MD5")

    // 对输入数据进行加密,过程是先将字符串中转换成byte数组,然后进行随机哈希
    val encoded: Array[Byte] = md5.digest(input.getBytes)

    // 将加密后的每个字节转化成十六进制，一个字节8位，相当于2个16进制，不足2位的前面补0
    encoded.map("%02x".format(_)).mkString
  }


  def loadConf(): Properties = {

    val properties = new Properties
    val ipstream = new FileInputStream("conf/config.properties")
    properties.load(ipstream)
    properties
  }

  def getMsgTime(msgTime: String) = {
    //    2020-08-17T09:14:58+08:00
    val str: String = msgTime.replaceAll("T|(\\+08:00)", " ")
      .substring(0,16)
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm")

    val now_min = str.substring(14, 16).toInt
    val timeStamp = df.parse(str.replaceAll(":[0-5][0-9]", s":${now_min / 5 * 5}")).getTime / 1000

    timeStamp
  }

  def main(args: Array[String]): Unit = {
    val str: String = MD5Encode("100.105.57.15959544220.249.46.438058.101.147.20462685").substring(8,24)
    println(str)
  }
}
