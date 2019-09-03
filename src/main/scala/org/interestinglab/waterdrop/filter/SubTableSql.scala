package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

class SubTableSql extends BaseFilter {

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

  private def checkSQLSyntax(sql: String): (Boolean, String) = {
    val sparkSession = SparkSession.builder.getOrCreate
    val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(sql)

    if (!logicalPlan.analyzed) {
      val logicPlanStr = logicalPlan.toString
      logicPlanStr.toLowerCase.contains("unresolvedrelation") match {
        case true => (true, "")
        case false => {
          val msg = "config[sql] cannot be passed through sql parser, sql[" + sql + "], logicPlan: \n" + logicPlanStr
          (false, msg)
        }
      }
    } else {
      (true, "")
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    val split = this.conf.getString("split")
    val user = this.conf.getString("user")
    val password = this.conf.getString("password")
    val url = this.conf.getString("url")
    val column = this.conf.getString("columns")
    val columns = column.replaceAll("\\s", "")
    val repartition = this.conf.getInt("repartition")
    val where = this.conf.getString("where")
    val partitionKeys = this.conf.getString("partitionKeys")
    val partitionValues = this.conf.getString("partitionValues")
    val hivedbtbls = this.conf.getString("hivedbtbls");

    val tables = df.collect().map(row => {
      row.getString(0)
    });
    tables.foreach(tableName => {
      val ds = spark.read.format("jdbc")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("url", url)
        .option("dbtable", s"(select max($split),min($split) from $tableName) tmp")
        .option("user", user)
        .option("password", password)
        .load()

      val length = ds.collect().length
      var lower: Long = 0L
      var upper: Long = Integer.MAX_VALUE
      val bound = length match {
        case 1 => {
          if (ds.collect()(0).schema.fields(0).dataType.isInstanceOf[IntegerType]) {
            val a = ds.collect()(0).getAs[Int](0)
            val b = ds.collect()(0).getAs[Int](1)
            lower = java.lang.Long.parseLong(String.valueOf(a))
            upper = java.lang.Long.parseLong(String.valueOf(b))
          } else if (ds.collect()(0).schema.fields(0).dataType.isInstanceOf[DecimalType]) {
            val a = ds.collect()(0).getAs[java.math.BigDecimal](0)
            val b = ds.collect()(0).getAs[java.math.BigDecimal](1)
            lower = java.lang.Long.parseLong(String.valueOf(a))
            upper = java.lang.Long.parseLong(String.valueOf(b))
          } else {
            lower = ds.collect()(0).getAs[Long](0)
            upper = ds.collect()(0).getLong(1)
          }
          (lower, upper)
        }
        case _ => {
          (lower, upper)
        }
      }
      println(s"bound is $bound")
      var array = ArrayBuffer[(String, String)]()
      val agg = Math.ceil((java.lang.Double.parseDouble(bound._1.toString) - java.lang.Double.parseDouble(bound._2.toString)) / java.lang.Double.parseDouble(repartition.toString))
      for (i <- 0 to repartition) {
        val end = i + 1;
        array += (((i * agg + bound._2).toString, (end * agg + bound._2).toString))
      }
      var predicates: ArrayBuffer[String] = new ArrayBuffer[String]()

      if (where != null && !"".equals(where)) {
        predicates = array.map { case (start, end) => s" $where and $split>= $start and $split < $end"
        }
      } else {
        predicates = array.map { case (start, end) => s" $split>= $start and $split < $end"
        }
      }
      val arr = predicates.toArray

      arr.foreach(println(_))

      val prop = new java.util.Properties
      prop.setProperty("user", user)
      prop.setProperty("password", password)
      prop.setProperty("driver", "com.mysql.jdbc.Driver")
      var frame = spark.read.jdbc(url, tableName, arr, prop)
      val strings = columns.split(",")
      val names = frame.schema.fieldNames
      names.foreach(field => {
        if (!strings.contains(field)) {
          frame = frame.drop(frame.col(field))
        }
      })
      val fields = frame.schema.fields
      val fieldNames = frame.schema.fieldNames
      var sb = new StringBuilder
      sb.append(s"CREATE TABLE IF NOT EXISTS $hivedbtbls (")
      fields.foreach(x => {
        val name = x.name
        sb.append(s"$name ")
        x.dataType match {
          case IntegerType | BooleanType | LongType | DecimalType() | ByteType | ShortType => {
            sb.append(s" bigint ,")
          }
          case FloatType | DoubleType => {
            sb.append(s" double ,")
          }
          case StringType | TimestampType | DateType => {
            sb.append(s" string ,")
          }
          case _ => {
            sb.append(s" string ,")
          }
        }
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
        sb.append("_shard string,")
        sb = sb.deleteCharAt(sb.length - 1)
        sb.append(")")
      } else {
        sb.append("PARTITIONED BY ( _shard string)")
      }
      sb.append("stored as parquet ")
      if(this.conf.hasPath("location")){
        if(this.conf.getString("location")!=null || !this.conf.getString("location").equals("")){
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
        sql.append(s" _shard = '$tableName'")
        sql.append(")")
      } else {
        sql.append(s"PARTITION (_shard='$tableName')")
      }

      val columnlist = fieldNames.mkString(",")

      sql.append(s" select $columnlist from $tableName ")

      println(sql.toString())
      log.info(s"####################### insert sql is : $sql #############################")

      frame.createOrReplaceTempView(tableName)

      spark.sql(sql.toString()).repartition(repartition)
    })
    df
  }
}
