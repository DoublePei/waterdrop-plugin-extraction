package org.interestinglab.waterdrop.input

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class SubTable extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("url", "user", "password");

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

    spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", config.getString("url"))
      .option("dbtable", "(select  table_name  from information_schema.tables where TABLE_SCHEMA = '" + config.getString("database") + "' and table_name regexp '" + config.getString("tableRegexp") + "' ) tmp")
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .load()
  }
}
