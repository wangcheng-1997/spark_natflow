package com.zjtuojing.natflow

import java.sql.{Connection, DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.Date

import com.zjtuojing.natflow.BeanClass.NATBean
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
 * ClassName NATFlow
 * Date 2020/8/12 9:16
 */
object NatFlow {

  def main(args: Array[String]): Unit = {
    val properties = MyUtils.loadConf()

    //TODO 参数校验
    if (args.length != 1) {
      System.err.println(
        """
          |com.zjtuojing.flowmsg.Flowmsg2HDFS
          |参数错误：
          |
          |batchDuration
        """.stripMargin)
      sys.exit()
    }

    val Array(batchDuration) = args
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      //      .setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", properties.getProperty("kafka.maxRatePerPartition"))
      .set("spark.streaming.backpressure.enabled", "true")
      .set("es.port", properties.getProperty("es.port"))
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.nodes.wan.only", properties.getProperty("es.nodes.wan.only"))
      .set("es.index.auto.create", properties.getProperty("es.index.auto.create"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sparkConf.registerKryoClasses(Array(classOf[NATBean]))

    val sc = SparkContext.getOrCreate(sparkConf)
    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sc.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))
    val ssc = new StreamingContext(sc, Seconds(batchDuration.toInt))

    // 加载配置信息
    // 指定数据库连接url，userName，password
    val url1 = properties.getProperty("mysql.url")
    val url2 = "jdbc:mysql://30.250.11.142:3306/broadband?characterEncoding=utf-8&useSSL=false"
    val url3 = "jdbc:mysql://30.250.60.35:3306/nat_log?characterEncoding=utf-8&useSSL=false"
    val userName1 = properties.getProperty("mysql.username")
    val password1 = properties.getProperty("mysql.password")
    //    ConnectionPool.singleton(url1, userName1, password1)
    //    ConnectionPool.add("offset",url1,userName1,password1)


    //注册Driver
    Class.forName("com.mysql.jdbc.Driver")
    //得到连接
    val offset_connection: Connection = DriverManager.getConnection(url1, userName1, password1)
    val username_connection = DriverManager.getConnection(url2, userName1, password1)
    val natlog_connection = DriverManager.getConnection(url3, userName1, password1)

    //    DBs.setupAll()

    //TODO 设置kafka相关参数
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> properties.getProperty("metadata.broker.list"),
      "auto.offset.reset" -> properties.getProperty("auto.offset.reset")
    )

    //TODO  NAT日志解析
    NATAnalyze(kafkaParams, "syslog", ssc, "syslog", "nat_offset", offset_connection, username_connection, natlog_connection)

    ssc.start()
    ssc.awaitTermination()


  }

  def NATAnalyze(params: Map[String, String], groupId: String, ssc: StreamingContext, topic: String, tableName: String, connection1: Connection, connection2: Connection, connection3: Connection): Unit = {
    //TODO 设置kafka相关参数
    val kafkaParams = params + ("group.id" -> groupId)
    val stream = createDirectStream(ssc, kafkaParams, topic, tableName, groupId, connection1)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //TODO 处理数据
    stream.foreachRDD(rdd => {
      val now = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
      val datetime = dateFormat.format(System.currentTimeMillis())

      if (!rdd.isEmpty()) {
        //获取rdd在kafka中的偏移量信息
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val statement1 = connection2.createStatement

        var usernames = Map[String, String]()
        val username = statement1.executeQuery(s"""select framedip,loginname from aaa_bb_online""")
        while (username.next) {
          usernames ++= Map(username.getString("framedip") -> username.getString("loginname"))
        }
        val users: Broadcast[Map[String, String]] = ssc.sparkContext.broadcast(usernames)

        rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

        rdd.saveAsTextFile(s"hdfs://nns/nat_log/${now.substring(0, 8)}/${now.substring(8, 10)}/${now.substring(10)}")

        val base = rdd.map(per => {
          val strings: Array[String] = per._2.split(",")
          strings
        })
          .filter(_.length >= 2)
          .map(per => {
            val strings1: Array[String] = per(0).split("\\s+")
            val strings2: Array[String] = per(1).split(" snat to ")
            val str = strings1(0).replaceAll("T", " ")
            val date: Long = dateFormat.parse(str.substring(0, 15) + str.substring(15, 16).toInt / 5 * 5 + ":00")
              .getTime / 1000
            val hostIP = strings1(1)
            val corresponds = strings1(6).split("[->()]")
            val sourceIpPort = corresponds(0)
            val targetIpPort = corresponds(2)
            val convertedIpPort = strings2(1)
            var protocol = ""
            if (corresponds(3).endsWith(":")) {
              protocol = corresponds(3).substring(0, corresponds(3).length - 1)
            } else {
              protocol = corresponds(3)
            }
            val sourceIp = sourceIpPort.split(":")(0)
            val sourcePort = sourceIpPort.split(":")(1)
            val targetIp = targetIpPort.split(":")(0)
            val targetPort = targetIpPort.split(":")(1)
            val convertedIp = convertedIpPort.split(":")(0)
            val convertedPort = convertedIpPort.split(":")(1)

            // 获取运营商
            var operate = "UnKnown"
            //获取省份
            var province = "UnKnown"
            //获取城市
            var city = "UnKnown"

            val maps = util.IpSearch.getRegionByIp(targetIp)

            if (!maps.isEmpty) {
              operate = maps.get("运营").toString
              province = maps.get("省份").toString
              city = maps.get("城市").toString
            }

            val username = users.value.getOrElse(sourceIp, "UnKnown")
            val rowkey = MyUtils.MD5Encode(sourceIp + sourcePort + targetIp + targetPort + convertedIp + convertedPort).substring(8, 24) + "_" + date

            NATBean(date, hostIP, sourceIp, sourcePort, targetIp, targetPort, protocol, convertedIp, convertedPort, operate, province, city, username, rowkey)
          }).persist(StorageLevel.MEMORY_AND_DISK_SER)

        val nat_count = base.count()

        val statement2: Statement = connection3.createStatement()

        statement2.executeUpdate(s"insert into nat_count (count_min,count_sec,update_time) values ('$nat_count','${nat_count / 300}','${datetime.substring(0, 15) + datetime.substring(15, 16).toInt / 5 * 5 + ":00"}')")

        val baseRDD = base.filter(_.username != "UnKnown")
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

        //TODO 1 province维度聚合
        val province = baseRDD.map(per => {
          ((per.accesstime, per.province), 1)
        }).reduceByKey(_ + _).map(per => Map("types" -> "province", "accesstime" -> per._1._1, "data" -> per._1._2, "count" -> per._2))


        //TODO 2 运营商维度聚合
        val operator = baseRDD.map(per => {
          ((per.accesstime, per.operator), 1)
        }).reduceByKey(_ + _).map(per => Map("types" -> "operator", "accesstime" -> per._1._1, "data" -> per._1._2, "count" -> per._2))


        //TODO 3 目标IP维度聚合
        baseRDD.map(per => {
          ((per.accesstime, per.targetIp), 1)
        }).reduceByKey(_ + _)
          .filter(_._2 > 10).map(per => (per._1._1, per._1._2, per._2))
          .coalesce(1)
          .saveAsTextFile(s"hdfs://nns/NATIp/${now.substring(0, 8)}/${now.substring(8)}")

        //TODO 4 省会维度聚合
        val city = baseRDD
          //          .filter(per => provincialCapitals.contains(per.city))
          .map(per => ((per.accesstime, per.city), 1))
          .reduceByKey(_ + _).map(per => Map("types" -> "city", "accesstime" -> per._1._1, "data" -> per._1._2, "count" -> per._2))

        EsSpark.saveToEs(province, s"bigdata_nat_flow_${now.substring(0, 8)}/nat")
        EsSpark.saveToEs(operator, s"bigdata_nat_flow_${now.substring(0, 8)}/nat")
        EsSpark.saveToEs(city, s"bigdata_nat_flow_${now.substring(0, 8)}/nat")

        val value: RDD[NATBean] = ssc.sparkContext.parallelize(baseRDD.collect()).persist(StorageLevel.MEMORY_AND_DISK_SER)

        //ES实现hbase二级索引
        val rowkeys: RDD[Map[String, Any]] = value.map(per => {
          Map("accesstime" -> per.accesstime,
            "sourceIp" -> per.sourceIp,
            "sourcePort" -> per.sourcePort,
            "targetIp" -> per.targetIp,
            "targetPort" -> per.targetPort,
            "convertedIp" -> per.convertedIp,
            "convertedPort" -> per.convertedPort,
            "rowkey" -> per.rowkey
          )
        })

        baseRDD.map(_.username).coalesce(1)
          .saveAsTextFile(s"hdfs://nns/nat_user/${now.substring(0, 8)}/${now.substring(8, 10)}/${now.substring(10)}")

        statement2.executeUpdate(s"insert into nat_hbase_count (count_5min,count_sec,update_time) values ('${rowkeys.count()}','${rowkeys.count() / 300}','${datetime.substring(0, 15) + datetime.substring(15, 16).toInt / 5 * 5 + ":00"}')")
        EsSpark.saveToEs(rowkeys, s"bigdata_nat_hbase_${now.substring(0, 8)}/hbase", Map("es.mapping.id" -> "rowkey"))

        val tableName = "syslog"
        val conf = HBaseConfiguration.create()
        val jobConf = new JobConf(conf)
        jobConf.set("hbase.zookeeper.quorum", "30.250.60.2,30.250.60.3,30.250.60.5,30.250.60.6,30.250.60.7")
        jobConf.set("zookeeper.znode.parent", "/hbase")
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
        jobConf.setOutputFormat(classOf[TableOutputFormat])

        value.map(per => {

          val rowkey = Bytes.toBytes(per.rowkey)

          val put = new Put(rowkey)
          put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("accesstime"), Bytes.toBytes(dateFormat.format(per.accesstime * 1000)))
          put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("hostIP"), Bytes.toBytes(per.hostIP))
          put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("sourceIp"), Bytes.toBytes(per.sourceIp))
          put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("sourcePort"), Bytes.toBytes(per.sourcePort))
          put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("targetIp"), Bytes.toBytes(per.targetIp))
          put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("targetPort"), Bytes.toBytes(per.targetPort))
          put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("protocol"), Bytes.toBytes(per.protocol))
          put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("convertedIp"), Bytes.toBytes(per.convertedIp))
          put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("convertedPort"), Bytes.toBytes(per.convertedPort))
          put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("username"), Bytes.toBytes(per.username))
          (new ImmutableBytesWritable, put)
        }).saveAsHadoopDataset(jobConf)

        base.unpersist()
        baseRDD.unpersist()

        offset2Mysql(offsetRanges, groupId, "nat_offset", connection1)
      }
    })
  }

  def offset2Mysql(offsetRanges: Array[OffsetRange], groupId: String, tableName: String, connection: Connection): Unit = {
    val statement = connection.createStatement

    for (o <- offsetRanges) {
      statement.executeUpdate(s"replace into $tableName(groupId, topic, partitionNum, offsets) VALUES('$groupId','${o.topic}','${o.partition}','${o.untilOffset}')")
    }

  }

  def createDirectStream(ssc: StreamingContext, kafkaParams: Map[String, String], topic: String, tableName: String, groupId: String, connection: Connection): InputDStream[(String, String)] = {

    val topics = topic.split(" ").toSet
    val statement = connection.createStatement

    //    获取自己维护的偏移量
    val rs = statement.executeQuery(s"""select * from $tableName where groupId = '$groupId'""")
    var fromOffsets = Map[TopicAndPartition, Long]()
    while (rs.next()) {
      fromOffsets ++= Map(TopicAndPartition(rs.getString("topic"), rs.getInt("partitionNum")) -> rs.getLong("offsets"))
    }
    val stream =
      if (fromOffsets.isEmpty) { // 假设程序第一次启动
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      } else {
        var checkedOffset = Map[TopicAndPartition, Long]()
        val kafkaCluster = new KafkaCluster(kafkaParams)
        val earliestLeaderOffsets: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kafkaCluster.getEarliestLeaderOffsets(fromOffsets.keySet)

        val latestLeaderOffsets = kafkaCluster.getLatestLeaderOffsets(fromOffsets.keySet)

        if (earliestLeaderOffsets.isRight) {
          val topicAndPartitionToOffset = earliestLeaderOffsets.right.get
          val topicAndPartitionLatestLeaderOffset = latestLeaderOffsets.right.get
          //           开始对比
          checkedOffset = fromOffsets.map(owner => {
            val clusterEarliestOffset = topicAndPartitionToOffset(owner._1).offset
            val clusterLateastOffset = topicAndPartitionLatestLeaderOffset(owner._1).offset

            if (owner._2 >= clusterEarliestOffset) {
              if (owner._2 <= clusterLateastOffset) {
                owner
              } else {
                (owner._1, clusterLateastOffset)
              }
            } else {
              (owner._1, clusterEarliestOffset)
            }
          })
        }
        // 程序非第一次启动
        val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, checkedOffset, messageHandler)
      }
    stream
  }

}
