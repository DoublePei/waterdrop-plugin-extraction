package org.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.interestinglab.waterdrop.util.RedisClientUtil

import scala.collection.JavaConversions._

class Redis extends BaseOutput {


  var config: Config = ConfigFactory.empty()


  /**
    * Set Config.
    **/
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
    * Get Config.
    **/
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    (true, "")
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

  }

  override def process(ds: Dataset[Row]): Unit = {

    val threads = config.hasPath("threads") match {
      case true => {
        config.getInt("threads")
      }
      case _ => 1000
    }
    val expire = config.getLong("expire")
    config.getString("type") match {

      case "string" => {
        ds.foreachPartition(rows => {
          val clientUtil = RedisClientUtil(config).getClient()
          val resource = clientUtil.getResource
          val pipeline = resource.pipelined()
          try {
            var i = 0
            rows.foreach(row => {
              if (i > threads) {
                pipeline.sync()
                pipeline.clear()
                i = 0
              }
              i = i + 1
              pipeline.set(row.getString(0), row.getString(1))
              if (expire > 0L) {
                pipeline.pexpire(row.getString(0), expire)
              }
            })
            pipeline.sync()

          } catch {
            case e => e.printStackTrace()
          } finally {
            clientUtil.close()
            pipeline.close()
          }
        })
      }
      case "hash" => {
        ds.foreachPartition(rows => {
          val clientUtil = RedisClientUtil(config).getClient()
          val resource = clientUtil.getResource
          val pipeline = resource.pipelined()
          try {
            var i = 0
            rows.foreach(row => {
              if (i > threads) {
                pipeline.sync()
                pipeline.clear()
                i = 0
              }
              i = i + 1
              val fields = row.schema.fields
              var maps: Map[String, String] = Map()
              var key = ""
              for (w <- 0 until fields.length) {
                if (w == 0) {
                  key = row.getString(0)
                } else {
                  maps += (row.schema.fields(w).name -> row.getString(w))
                }
              }
              pipeline.hmset(key, maps)
              if (expire > 0L) {
                pipeline.pexpire(row.getString(0), expire)
              }
            })
            pipeline.sync()
          } catch {
            case e => e.printStackTrace()
          } finally {
            clientUtil.close()
            pipeline.close()
          }
        })
      }
      case "set" => {
        ds.foreachPartition(rows => {
          val clientUtil = RedisClientUtil(config).getClient()
          val resource = clientUtil.getResource
          val pipeline = resource.pipelined()
          try {
            var i = 0
            rows.foreach(row => {
              if (i > threads) {
                pipeline.sync()
                pipeline.clear()
                i = 0
              }
              i = i + 1
              pipeline.sadd(row.getString(0), row.getString(1))
              if (expire > 0L) {
                pipeline.pexpire(row.getString(0), expire)
              }
            })
            pipeline.sync()

          } catch {
            case e => e.printStackTrace()
          } finally {
            clientUtil.close()
            pipeline.close()
          }
        })
      }
      case _ => {
        throw new Exception("暂时不支持的数据类型")

      }
    }
  }
}

