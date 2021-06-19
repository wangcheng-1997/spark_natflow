package com.zjtuojing.utils

import com.alibaba.fastjson.JSON
import redis.clients.jedis.{Jedis, JedisPool}

object JedisPool {

  val properties = MyUtils.loadConf()
  val password = properties.getProperty("redis.password")

  val TIME_OUT = 30000

  def getJedisPool(): JedisPool = {

    val redisHost = properties.getProperty("redis.host")
    val redisPort = properties.getProperty("redis.port")

    val pool = new JedisPool(redisHost,redisPort.toInt)

    pool
  }

  def getJedisClient(pool: JedisPool): Jedis = {
    val jedis = pool.getResource
    jedis.auth(password)
    jedis
  }

  def getJedisClient(): Jedis = {
    val redisHost = properties.getProperty("redis.host")
    val redisPort = properties.getProperty("redis.port")

    val pool = new JedisPool(redisHost,redisPort.toInt)
    val jedis = pool.getResource
    jedis.auth(password)
    jedis
  }

  def main(args: Array[String]): Unit = {

    val jedisPool = getJedisPool()
    val jedis = getJedisClient(jedisPool)
    val map = jedis.hgetAll("nat:radius")
      .values().toArray
      .map(json => {
        val jobj = JSON.parseObject(json.toString)
        (jobj.getString("account"), jobj.getString("ip"))
      }).toMap

    println(map.getOrElse("4,-(+A69yq23102308","null"))

    jedis.close()
  }


}

