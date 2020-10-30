package com.zjtuojing.natflow

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql.{EsSparkSQL, _}
import org.slf4j.{Logger, LoggerFactory}

/**
 * ClassName ESDnsFTP
 * Date 2020/6/2 17:28
 */
object ESRatioTransport {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val properties = MyUtils.loadConf()

    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ESAggregate")
      .setMaster("local[*]")
      .set("es.port", properties.getProperty("es.port"))
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.nodes.wan.only", properties.getProperty("es.nodes.wan.only"))
      .set("es.index.auto.create", properties.getProperty("es.index.auto.create"))
      //      .set("spark.es.input.use.sliced.partitions", properties.getProperty("spark.es.input.use.sliced.partitions"))
      .set("es.index.read.missing.as.empty", "true")
    //采用kryo序列化库
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册
    sparkConf.registerKryoClasses(
      Array(
        classOf[Array[String]]
      )
    )

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    // TODO 1 ratio 大量数据迁移
    // 获取时间范围
    for (upper <- 1591389600 until 1590940800 by -600) {
      val lower = upper - 600
      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}],\"must_not\":[{\"term\":{\"types\":\"responseCode\"}},{\"term\":{\"types\":\"responseType\"}},{\"term\":{\"types\":\"requestType\"}},{\"term\":{\"types\":\"qps\"}}]}}}"""

      //      values.take(3).foreach(println)
      val frame: DataFrame = EsSparkSQL.esDF(spark, "bigdata_dns_flow_ratio", query)
      frame.saveToEs("bigdata_dns_flow_ratio_202006/ratio")

    }

//     TODO 2 ratio 小量数据迁移
//     TODO 06.15- 07.01
//     获取时间范围
//     查询语句
//              val query =
//            """
//              |{
//              |"query":{
//              |"bool":{
//              |"must":[{"range":{"accesstime":{"gte":"1590940800","lt":"1592150400"}}}],"must_not":[{"match":{"types":"responseCodeDomain"}},{"match":{"types":"responseCodeClientIP"}},{"match":{"types":"responseCodeAuthorityDomain"}}],"should":[]}}
//              |}
//              |}
//              |""".stripMargin
//
//        val frame: DataFrame = EsSparkSQL.esDF(spark, "bigdata_dns_flow_ratio", query)
//        frame.saveToEs("bigdata_dns_flow_ratio_v1/ratio")

    // TODO 1 top 大量数据迁移
    // 获取时间范围
//    for (upper <- 1590940800 until 1575129600 by -7200) {
//      val lower = upper - 7200
//      // 查询语句
//      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}],\"must_not\":[{\"term\":{\"types\":\"user\"}}]}}}"""
//
//      //      values.take(3).foreach(println)
//      val frame: DataFrame = EsSparkSQL.esDF(spark, "bigdata_dns_flow_top_v1", query)
//      frame.saveToEs("bigdata_dns_flow_top_v2/aip")
//
//    }

    sc.stop()
  }
}
