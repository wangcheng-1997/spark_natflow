package com.zjtuojing.natflow

import java.sql.DriverManager
import java.util

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.apache.hadoop.hbase.client.{Get, HTable, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql.EsSparkSQL
import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, JedisPoolConfig, JedisSentinelPool}
import scalikejdbc.{ConnectionPool, DB, SQL}
import scalikejdbc.config.DBs


/**
 * ClassName IntelliJ IDEA
 * Date 2020/10/16 15:26
 */
object HbaseTest {
  def main(args: Array[String]): Unit = {
    val properties = MyUtils.loadConf()
    val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setTestOnBorrow(true)
    jedisPoolConfig.setTestOnReturn(true)
    jedisPoolConfig.setTestWhileIdle(true)
    jedisPoolConfig.setMaxTotal(-1)

    val sentinelSet = new util.HashSet[String]()
    val redisUrl = properties.getProperty("redis.url")
    val urls: Array[String] = redisUrl.split(",")
    for (i <- urls.indices) {
      sentinelSet.add(urls(i))
    }

    val password = properties.getProperty("redis.password")

    val pool = new JedisSentinelPool("mymaster", sentinelSet, jedisPoolConfig, 30000, password)

    val jedis: Jedis = pool.getResource
    val stringToString: Map[String, String] = getPhoneNumAndAddress(jedis)
      .filter(per => per._2.split("\t")(1) != "未知" )
//    614771 4943
    println(s"输出数据...${stringToString.size}")
    for (elem <- stringToString) {
      println(elem._1,elem._2)
    }


//    val sparkConf = new SparkConf()
//      .setAppName(this.getClass.getSimpleName)
//      .setMaster("local[*]")
//      .set("spark.streaming.kafka.maxRatePerPartition", properties.getProperty("kafka.maxRatePerPartition"))
//      .set("es.port", properties.getProperty("es.port"))
//      .set("es.nodes", properties.getProperty("es.nodes"))
//      .set("es.nodes.wan.only", properties.getProperty("es.nodes.wan.only"))
//      .set("es.index.auto.create", properties.getProperty("es.index.auto.create"))
//
//    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//    val sc: SparkContext = spark.sparkContext
//
//    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
//    sc.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
//    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
//    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
//    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
//    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))
//
////    val str = "" + "\t" + ""
////    val strings: Array[String] = str.split("\t")
////    println(strings(0),strings(1),strings.length)
//
//    val table: HTable = HbaseUtils.getHTable("30.250.60.2,30.250.60.3,30.250.60.5,30.250.60.6,30.250.60.7", "2181", "syslog")
//    table.setAutoFlush(false, false)
//    table.setWriteBufferSize(1024 * 1024 * 3)
//    val rowkey = Bytes.toBytes("f4668477e10540b1_1603398000")
//
//    val get = new Get(rowkey)
//    val result: util.List[Cell] = table.get(get).listCells()
//
//    val value: util.Iterator[Cell] = result.iterator()
//    while (value.hasNext){
//      println {
//        "%s: %s".format(new String(value.next().getQualifier), new String(value.next().getValue))
//      }
//    }



//    Class.forName("com.mysql.jdbc.Driver")
////     指定数据库连接url，userName，password
//
//    val userName = properties.getProperty("mysql.username")
//
//    val url = "jdbc:mysql://30.250.60.35:3306/dns_analyze?characterEncoding=utf-8&useSSL=false"
//
//    val password = properties.getProperty("mysql.password")
//
//    ConnectionPool.singleton(url, userName, password)
//
//    DBs.setupAll()
//
//    val query =
//      """
//        |{
//        |  "query": {
//        |    "bool": {
//        |      "must": [
//        |        {
//        |          "range": {
//        |            "resolver": {
//        |              "gte": "0"
//        |            }
//        |          }
//        |        }
//        |      ]
//        |    }
//        |  }
//        |}
//        |""".stripMargin
//
//    val rdd = EsSparkSQL.esDF(spark, "bigdata_dns_flow_clear_dd", query)
//      .rdd
//      .map(per => {
//        val domain: String = per.getAs("domain").asInstanceOf[String]
//        val aip: String = per.getAs("aip").asInstanceOf[String]
//        val resolver: Long = per.getAs("resolver").asInstanceOf[Long]
//        ((domain, aip), resolver)
//      })
//      .reduceByKey(_ + _)
//      .filter(_._1._1!= null)
//      .filter(_._1._2!="0.0.0.0")
//      .filter(!_._1._1.startsWith("王梓"))
//      .sortBy(-_._2)
//      .take(1000000).toList
//
//
//
//    DB.localTx { implicit session =>
//      //存储偏移量
//      for (o: ((String, String), Long) <- rdd) {
//        SQL("insert into domain_aip (domain,aip,resolver) values (?,?,?) ")
//          .bind(o._1._1, o._1._2, o._2)
//          .update()
//          .apply()
//      }
//    }
//
//    DBs.closeAll()


//    sc.stop()
  }


  def getPhoneNumAndAddress(jedis: Jedis): Map[String, String] = {
    var maps = Map[String, String]()
    try {
      val redisArray = jedis.hgetAll("broadband:userinfo").values().toArray()
      maps = redisArray.map(array => {
        val jobj = JSON.parseObject(array.toString)
        var mobile = "未知"
        if(jobj.getString("mobile")!=""){
          mobile = jobj.getString("mobile")
        }
        (jobj.getString("username"), jobj.getString("doorDesc") + "\t" + mobile)

      }).toMap
    } catch {
      case e: Exception => {
        maps = maps
                e.printStackTrace()
      }
    }
    maps
  }

}
