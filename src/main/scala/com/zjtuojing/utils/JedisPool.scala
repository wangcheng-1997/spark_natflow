package com.zjtuojing.utils

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
    println(jedis.exists("nat:radius"))
    jedis.hgetAll("nat:radius")
      .values().toArray
      .take(1).foreach(println(_))


    jedis.close()
  }


}

