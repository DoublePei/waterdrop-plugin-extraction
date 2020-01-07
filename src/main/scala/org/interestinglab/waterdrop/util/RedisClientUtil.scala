package org.interestinglab.waterdrop.util

import java.util

import com.typesafe.config.Config
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisPool, JedisPoolConfig}


class RedisClientUtil(createRedisClient: () => JedisPool) extends Serializable {
  lazy val client = createRedisClient()

  def getClient(): JedisPool = {
    client
  }
}

object RedisClientUtil {
  private val connectionPoolTimeout = 3000
  private val soTimeOut = 8000
  private val maxAttempts = 2

  def apply(config: Config): RedisClientUtil = {
    val f = () => {
      val host = config.getString("host")
      var port = "6379"
      val gopconfig = new GenericObjectPoolConfig
      gopconfig.setMaxIdle(20)
      gopconfig.setMinIdle(8)
      gopconfig.setMaxWaitMillis(20000)
      gopconfig.setTestOnBorrow(true)
      gopconfig.setMaxTotal(100)
      if (config.hasPath("port")) {
        port = config.getString("port")
      }
      val client = config.hasPath("password") match {
        case true => {
          val client = new JedisPool(gopconfig, host, port.toInt, soTimeOut, config.getString("password"))
          client
        }
        case false => {
          val client = new JedisPool(gopconfig, host, port.toInt, soTimeOut)
          client
        }
      }
      sys.addShutdownHook {
        client.close()
      }
      client
    }
    new RedisClientUtil(f)
  }
}