package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.{Failure, Success, Try}

class SyncToHive extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  /**
    * Set Config.
    **/
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
    * Get Config.
    **/
  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("table_name") match {
      case true => {
        if (conf.hasPath("table_name")) {
          logWarning("parameter [table_name] is deprecated since 1.4")
        }
        (true, "")
      }
      case false => (true, "")
    }

  }

  override def process(spark: SparkSession, dfs: Dataset[Row]): Dataset[Row] = {

    val repartition = this.conf.getInt("repartition")
    val tableName = this.conf.getString("table_name")
    val columns = this.conf.getString("columns")
    val partitionKeys = this.conf.getString("partitionKeys")
    val partitionValues = this.conf.getString("partitionValues").toString
    val hivedbtbls = this.conf.getString("hivedbtbls");
    val df = dfs.repartition(repartition)
    val fields = df.schema.fields
    val fieldNames = df.schema.fieldNames
    var sb = new StringBuilder
    sb.append(s"CREATE TABLE IF NOT EXISTS $hivedbtbls (")
    fields.foreach(x => {
      val name = x.name
      val option = x.getComment()
      sb.append(s"$name ")
      x.dataType match {
        case IntegerType | BooleanType | LongType | ByteType | ShortType => {
          sb.append(s" bigint ")
        }
        case FloatType | DoubleType => {
          sb.append(s" double ")
        }
        case StringType | TimestampType | DateType => {
          sb.append(s" string ")
        }
        case dt: DecimalType => {
          dt.scale match {
            case 0 => {
              sb.append(s" bigint ")
            }
            case _ => {
              sb.append(s" double ")
            }
          }
        }
        case _ => {
          sb.append(s" string ")
        }
      }
      val comment = option match {
        case Some(s) => {
          s
        }
        case None => {
          ""
        }
      }
      sb.append(s" comment '$comment',")
    })
    sb = sb.deleteCharAt(sb.length - 1)
    sb.append(")")
    if (partitionKeys != null && !"".equals(partitionKeys)) {
      val keys = partitionKeys.split(",")
      sb.append("PARTITIONED BY (")
      for (i <- 0 until keys.length) {
        val key = keys(i)
        sb.append(s" $key string ,")
      }
      sb = sb.deleteCharAt(sb.length - 1)
      sb.append(")")
    }
    sb.append("stored as parquet ")
    if (this.conf.hasPath("location")) {
      if (this.conf.getString("location") != null || !this.conf.getString("location").equals("")) {
        val location = this.conf.getString("location")
        sb.append(s" location '$location'")
      }
    }

    println(sb.toString())
    log.info(s"####################### create sql is : $sb #############################")
    spark.sql(sb.toString())
    var sql = new StringBuilder
    sql.append(s"insert overwrite table $hivedbtbls ")
    if (partitionKeys != null && !"".equals(partitionKeys)) {
      val keys = partitionKeys.split(",")
      val values = partitionValues.split(",")
      sql.append("PARTITION (")
      for (i <- 0 until keys.length) {
        val key = keys(i)
        val value = values(i)
        sql.append(s" $key=$value,")
      }
      sql = sql.deleteCharAt(sql.length - 1)
      sql.append(")")
    }

    val cloumns = columns.split(",").map(e => e.trim).map(filed => {
      val v = filed.replaceAll("\\s+(as|AS)\\s+", " as ")
      if (v.contains(" as ")) {
        v
      } else {
        s"`$v`"
      }
    }).mkString(",")

    sql.append(s" select $cloumns from $tableName ")

    println(sql.toString())
    log.info(s"####################### insert sql is : $sql #############################")

    df.createOrReplaceTempView(tableName.toString)
    create_task(spark.sql(sql.toString())) match {
      case Success(ds) => ds
      case Failure(f) => {
        println(f)
        System.exit(1)
        df
      }
    }
  }

  def create_task(ds: Dataset[Row]): Try[Dataset[Row]] = {
    Try(ds)
  }
}
