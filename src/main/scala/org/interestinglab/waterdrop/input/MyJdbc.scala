package org.interestinglab.waterdrop.input

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class MyJdbc extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("url", "table", "user", "password");

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

    val ds = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", config.getString("url"))
      .option("dbtable", "(select max(" + config.getString("column") + "),min(" + config.getString("column") + ") from " + config.getString("table") + ") tmp")
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
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
    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", config.getString("url"))
      .option("dbtable", config.getString("table"))
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .option("partitionColumn", config.getString("column"))
      .option("numPartitions", config.getString("repartition"))
      .option("lowerBound", bound._2)
      .option("upperBound", bound._1)
      .load()
  }
}
