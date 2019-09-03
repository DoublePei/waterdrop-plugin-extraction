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

    //包含key 并且选择了redis的存储的类型
    config.hasPath("hosts") && config.hasPath("key") && config.hasPath("type") match {

      case true => {
        config.getString("type") match {

          case "string" => {
            val bool = config.hasPath("value")
            (bool, "string 类型创建结果：" + bool)
          }
          case "hash" => {
            val bool = config.hasPath("value")
            (bool, "hash 类型创建结果：" + bool)
          }
          case "list" => {
            (false, "list 正在开发中。。。")
          }
          case "set" => {
            (false, "set 正在开发中。。。")
          }
          case "zset" => {
            (false, "zset 正在开发中。。。")
          }
          case _ => {
            (false, "unknown redis-type ")
          }
        }
      }
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

  }

  override def process(ds: Dataset[Row]): Unit = {

    ds.schema.size match {
      case 2 => {
        ds.foreachPartition(rows => {
          val clientUtil = RedisClientUtil(config).getClient()
          val resource = clientUtil.getResource
          val pipeline = resource.pipelined()
          try {
            var i = 0
            rows.foreach(row => {
              if (i > 100000) {
                pipeline.sync()
                pipeline.clear()
                i = 0
              }
              pipeline.set(row.getString(0), row.getString(1))
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
      case 3 => {
        ds.foreachPartition(rows => {
          val clientUtil = RedisClientUtil(config).getClient()
          val resource = clientUtil.getResource
          val pipeline = resource.pipelined()
          try {
            var i = 0
            rows.foreach(row => {
              if (i > 10000) {
                pipeline.sync()
                pipeline.clear()
                i = 0
              }
              val maps = Map(row.schema.fields(1).name -> row.getString(1), row.schema.fields(2).name -> row.getString(2).toString)
              pipeline.hmset(row.getString(0), maps)
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
        throw new RuntimeException(s"Text data source supports only 2 columns," +
          s" and you have ${ds.schema.size} columns.")
      }
    }

  }

}

