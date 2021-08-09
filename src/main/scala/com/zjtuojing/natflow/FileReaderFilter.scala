package com.zjtuojing.natflow

import com.zjtuojing.utils.{JedisPool, MyUtils}
import org.apache.hadoop.fs.{FileStatus, FileUtil, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object FileReaderFilter {
  def main(args: Array[String]): Unit = {
    val properties = MyUtils.loadConf()
    try {
      val jedisPool = JedisPool.getJedisPool()
      val jedis = JedisPool.getJedisClient(jedisPool)
    } catch {
      case e: Exception => e.printStackTrace()
    }


    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sc.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))

    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    val files = hdfs.listFiles(new Path("/nat_log/20210805/05/06/2021_08_05_050458_a457ef6f4060_hssw-olrs-log-23.log"), true)
    while (files.hasNext) {
      val file = files.next()
//      if (file.getAccessTime>=(1628127900000L-300000)&& file.getAccessTime<1628127900000L){
//        println(file.getPath)
//      }
      println(file.getModificationTime)
    }

  }
}