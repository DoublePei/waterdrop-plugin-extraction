package org.interestinglab.waterdrop.input

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.types.{DecimalType, IntegerType, StructType}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.internal.util.TableDef.Column

class MyJdbcById extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("url", "table_name", "user", "password");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.isEmpty) {
      (true, "")
    } else {
      (
        false,
        "please specify " + nonExistsOptions
          .map { case (field, _) => "[" + field + "]" }
          .mkString(", ") + " as non-empty string")
    }
  }


  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val split = config.getString("split")
    val tableName = config.getString("table_name")
    val user = config.getString("user")
    val password = config.getString("password")
    val url = config.getString("url")
    val columns = config.getString("columns")
    val repartition = config.getInt("repartition")
    val where = config.getString("where")
    val database = config.getString("database")
    val driver = config.hasPath("driver") match {
      case true => {
        val tmp = config.getString("driver")
        var sql = ""
        if (tmp != null && !"".equals(tmp)) {
          sql = tmp
        } else {
          sql = "com.mysql.jdbc.Driver"
        }
        sql
      }
      case false => "com.mysql.jdbc.Driver"
    }


    var sql = ""
    if (where != null && !"".equals(where)) {
      sql = s"select max($split),min($split) from $tableName where $where"
    } else {
      sql = s"select max($split),min($split) from $tableName"
    }
    val ds = spark.read.format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", s"($sql) tmp")
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
          if (a == null || a.equals("null")) {
            lower = 0l
            upper = 0l
          } else {
            val b = ds.collect()(0).getAs[Int](1)
            lower = java.lang.Long.parseLong(String.valueOf(a))
            upper = java.lang.Long.parseLong(String.valueOf(b))
          }
        } else if (ds.collect()(0).schema.fields(0).dataType.isInstanceOf[DecimalType]) {
          val a = ds.collect()(0).getAs[java.math.BigDecimal](0)
          if (a == null || a.equals("null")) {
            lower = 0l
            upper = 0l
          } else {
            val b = ds.collect()(0).getAs[java.math.BigDecimal](1)
            lower = java.lang.Long.parseLong(String.valueOf(a))
            upper = java.lang.Long.parseLong(String.valueOf(b))
          }
        } else {
          val a = ds.collect()(0).getAs[Long](0);
          if (a == null || a.equals("null")) {
            lower = 0l
            upper = 0l
          } else {
            lower = ds.collect()(0).getAs[Long](0)
            upper = ds.collect()(0).getAs[Long](1)
          }
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
    val prop = new java.util.Properties
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    prop.setProperty("driver", driver)

    var frame = spark.read.jdbc(url, tableName, arr, prop)

    val tmp = tableName.replaceAll("`", "")
    if (driver.contains("mysql")) {
      val comment = spark.read.format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", s"(select COLUMN_NAME,COLUMN_COMMENT from information_schema.columns where table_name = '$tmp' and table_schema = '$database' ) simple")
        .option("user", user)
        .option("password", password)
        .load()
      var map: Map[String, String] = Map()
      comment.collect().foreach(x => {
        val value = x.getString(0)
        val value1 = x.getString(1).replaceAll("\'", "")
        map.+=(value -> value1)
      })
      val schemas = frame.schema.map(s => {
        s.withComment(map(s.name))
      })
      frame = spark.createDataFrame(frame.rdd, StructType(schemas)).repartition(repartition)
    }
    val strings = columns.split(",").map(s=>s.trim)
    val names = frame.schema.fieldNames

    names.foreach(field => {
      if (!strings.contains(field)) {
        frame = frame.drop(frame.col(field))
      }
    })
    frame
  }
}
