package com.zjtuojing.natflow

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.zjtuojing.utils.{ClickUtils, MyUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LivenessRpt {
  def main(args: Array[String]): Unit = {
    val properties = MyUtils.loadConf()
    val datetime = MyUtils.getTaskTime._3._2
    val date = new SimpleDateFormat("yyyyMMdd").format(datetime * 1000)

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
      .set("spark.speculation", "false")
      .set("spark.locality.wait", "10")
      .set("spark.storage.memoryFraction", "0.3")

    //采用kryo序列化库
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册
    conf.registerKryoClasses(
      Array(
        classOf[Array[String]],
        classOf[Map[String, Any]]
      ))
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    //HADOOP HA
    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sc.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))

    val value = sc.textFile(s"hdfs://nns/nat_user/$date/*/*")
      .coalesce(36)
      .map(per => {
        val users = per
        val str = per.substring(1, users.length - 1).split(",")
        if (str.size==2) (str(0),str(1).toInt)
        else (str(0)+","+str(1),str(2).toInt)
      })
      .reduceByKey(_ + _)
      .sortBy(_._2, false, 1)
      .map(per => (per._1, per._2, new Timestamp(datetime * 1000)))

//    value.foreach(println)

    import spark.implicits._

    val userDF = value.toDF("username", "resolver", "accesstime")

    ClickUtils.clickhouseWrite(userDF, "nat_log.nat_liveness")


  }
}
