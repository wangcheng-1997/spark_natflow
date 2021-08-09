package com.zjtuojing.natflow

import java.text.SimpleDateFormat

import com.zjtuojing.natflow.BeanClass.{NATBean, NATReportBean}
import com.zjtuojing.utils.MyUtils
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object NatFlowOffline {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val properties = MyUtils.loadConf()

  def main(args: Array[String]): Unit = {

    val Array(startTime, endTime) = args

    val conf = new SparkConf()
    conf.setAppName("natflow_offline")
    //    conf.setMaster("local[4]")

    //采用kryo序列化库
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册
    conf.registerKryoClasses(
      Array(
        classOf[NATBean],
        classOf[NATReportBean],
        classOf[ImmutableBytesWritable]
      )
    )
    val sparkContext = SparkContext.getOrCreate(conf)
    //HDFS HA
    sparkContext.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sparkContext.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sparkContext.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sparkContext.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))

    //创建SparkSQL实例
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()


    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for (t <- startTime.toLong to endTime.toLong by 300000) {
      logger.info(s"aaaa----${t},  ${simpleDateFormat.format(t)}")
      DelayNatFlow.call(sparkContext, sparkSession, t)
    }

  }
}
