package com.zjtuojing.utils

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{HTable, Table, _}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkHbaseBulkload {

  def main(args: Array[String]) = {
    val properties = MyUtils.loadConf()

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sc.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "30.254.234.31,30.254.234.33,30.254.234.35,30.254.234.36,30.254.234.38")
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.client.retries.number", "3")
    conf.set("hbase.rpc.timeout", "200000")
    conf.set("hbase.client.operation.timeout", "300000")
    conf.set("hbase.client.scanner.timeout.period", "100000")


    val seq = Seq(
      "7,stu,name,libai",
      "8,stu,name,baiqi",
      "3,stu,gender,boy",
      "3,stu,g=nder,boy",
      "3,stu,gen=r,boy",
      "3,stu,g]der,boy",
      "1,stu,name,erhu"
    )

    val source = sc.parallelize(seq)
      .map {
      x => {
        val splited = x.split(",")
        val rowkey = splited(0)
        val cf = splited(1)
        val clomn = splited(2)
        val value = splited(3)
        (rowkey, cf, clomn, value)
      }
    }.sortBy(per => (per._1+per._2,per._3))
    val rdd = source.map(x => {
      //将rdd转换成HFile需要的格式,我们上面定义了Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
      //KeyValue的实例为value
      //rowkey
      val rowKey = x._1
      val family = x._2
      val colum = x._3
      val value = x._4
      (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes(family), Bytes.toBytes(colum), Bytes.toBytes(value)))
    })
    //生成的HFile的临时保存路径
    val stagingFolder = "hdfs://30.254.234.33:8020/hfile_tmp"
    //将日志保存到指定目录
    rdd.saveAsNewAPIHadoopFile(stagingFolder,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)
    //此处运行完成之后,在stagingFolder会有我们生成的Hfile文件

    //开始即那个HFile导入到Hbase,此处都是hbase的api操作
    val load = new LoadIncrementalHFiles(conf)
    //hbase的表名
    val tableName = "student"
    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val conn = ConnectionFactory.createConnection(conf)
    //根据表名获取表
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    try {
      //创建一个hadoop的mapreduce的job
      val job = Job.getInstance(conf)
      //设置job名称
      job.setJobName("DumpFile")
      //此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      //输出文件的内容KeyValue
      job.setMapOutputValueClass(classOf[KeyValue])
      //配置HFileOutputFormat2的信息
      HFileOutputFormat2.configureIncrementalLoadMap(job, table)
      //开始导入
      val start = System.currentTimeMillis()
      load.doBulkLoad(new Path(stagingFolder), table.asInstanceOf[HTable])
      val end = System.currentTimeMillis()
      println("用时：" + (end - start) + "毫秒！")
    } finally {
      table.close()
      conn.close()
    }
  }
}