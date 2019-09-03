package org.interestinglab.waterdrop.input

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

class MyJdbcByMod extends BaseStaticInput {

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

    val split = config.getString("split");

    val column = config.getString("columns");

    val columns = column.replaceAll("\\s", "")
    val repartition = config.getInt("repartition");
    val where = config.getString("where")
    var array = ArrayBuffer[(String)]()
    for (i <- 0 until repartition) {
      array += (i.toString)
    }
    var predicates: ArrayBuffer[String] = new ArrayBuffer[String]()

    if (where != null && !"".equals(where)) {
      predicates = array.map { case (start) => s" $where and mod($split,$repartition) = $start "
      }
    } else {
      predicates = array.map { case (start) => s" mod($split,$repartition) = $start "
      }
    }
    val arr = predicates.toArray
    val prop = new java.util.Properties
    prop.setProperty("user", config.getString("user"))
    prop.setProperty("password", config.getString("password"))
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    var frame = spark.read.jdbc(config.getString("url"), config.getString("table_name"), arr, prop)
    val strings = columns.split(",")
    val names = frame.schema.fieldNames
    names.foreach(field => {
      if (!strings.contains(field)) {
        frame = frame.drop(frame.col(field))
      }
    })
    frame
  }
}
