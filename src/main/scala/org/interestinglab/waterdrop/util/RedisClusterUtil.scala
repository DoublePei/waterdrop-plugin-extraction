package org.interestinglab.waterdrop.util

import java.util

import com.typesafe.config.Config
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}


class RedisClusterUtil(createRedisClient: () => JedisCluster) extends Serializable {
  lazy val client = createRedisClient()

  def getClient(): JedisCluster = {
    client
  }
}

object RedisClusterUtil {
  private val connectionPoolTimeout = 3000
  private val soTimeOut = 3000
  private val maxAttempts = 2
  def apply(config: Config): RedisClusterUtil = {
    val f=() => {
      val set = new util.HashSet[HostAndPort]()
      val str = config.getString("hosts")
      var port = "6379"
      val jpc: JedisPoolConfig = new JedisPoolConfig()
      jpc.setMaxIdle(20)
      jpc.setMinIdle(8)
      jpc.setMaxWaitMillis(2000)
      jpc.setTestOnBorrow(true)
      jpc.setMaxTotal(100)
      if (config.hasPath("port")) {
        port = config.getString("port")
      }
      val strings = str.split(",")
      strings.foreach(host => {
        set.add(new HostAndPort(host, port.toInt))
      })
      val client = config.hasPath("password") match {
        case true => {
          val client = new JedisCluster(set, connectionPoolTimeout, soTimeOut, maxAttempts, config.getString("password"), jpc)
          client
        }
        case false => {
          val client = new JedisCluster(set, connectionPoolTimeout, soTimeOut, maxAttempts, jpc)
          client
        }
      }
      sys.addShutdownHook {
        client.close()
      }
      client
    }
    new RedisClusterUtil(f)
  }
}