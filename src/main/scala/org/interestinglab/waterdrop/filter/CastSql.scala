package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class CastSql extends BaseFilter {

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

    val repartition = this.conf.hasPath("repartition") && StringUtils.isNotBlank(this.conf.getString("repartition")) match {
      case true => Integer.parseInt(this.conf.getString("repartition"))
      case false => 200
    }
    this.conf.hasPath("table_name") && StringUtils.isNotBlank(this.conf.getString("table_name")) match {
      case true => {
//        val newSchema = df.schema.map(schema => {
//          if (schema.dataType.isInstanceOf[IntegerType]) {
//            new StructField(schema.name, LongType
//              , schema.nullable, schema.metadata)
//          } else if (schema.dataType.isInstanceOf[BooleanType]) {
//            new StructField(schema.name, LongType
//              , schema.nullable, schema.metadata)
//          } else if (schema.dataType.isInstanceOf[TimestampType]) {
//            new StructField(schema.name, StringType
//              , schema.nullable, schema.metadata)
//          } else {
//            schema
//          }
//        })
//        val frame = spark.createDataFrame(df.rdd, StructType(newSchema)).repartition(repartition)
        df.createOrReplaceTempView(this.conf.getString("table_name"))
      }
      case false => {}
    }
    spark.sql(conf.getString("sql")).repartition(repartition)
  }
}
