package org.interestinglab.waterdrop.output

import java.sql.{Connection, DriverManager}

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SaveMode}

class MCD extends BaseOutput {
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

    val requiredOptions = List("url", "table", "user", "password");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.length == 0) {

      val saveModeAllowedValues = List("overwrite", "append", "ignore", "error");

      if (!config.hasPath("save_mode") || saveModeAllowedValues.contains(config.getString("save_mode"))) {
        (true, "")
      } else {
        (false, "wrong value of [save_mode], allowed values: " + saveModeAllowedValues.mkString(", "))
      }

    } else {
      (false, "please specify " + nonExistsOptions.map("[" + _._1 + "]").mkString(", ") + " as non-empty string")
    }
  }

  def getConnection(): Connection = {
    var conn: Connection = null
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(config.getString("url")
        , config.getString("user"), config.getString("password"))
    }
    conn
  }

  def validateTableExist(db: String, tb: String): Boolean = {
    var conn: Connection = null
    var flag = false
    try {
      conn = getConnection()
      val meta = conn.getMetaData
      val t = Array("TABLE")
      val rs = meta.getTables(null, null, db + "." + tb, t)
      flag = rs.next
    } catch {
      case e => e.printStackTrace()
    }
    finally {
      if (conn != null) {
        conn.close()
      }
    }
    flag
  }

  override def process(df: Dataset[Row]): Unit = {
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", config.getString("user"))
    prop.setProperty("password", config.getString("password"))

    val saveMode = config.getString("save_mode")
    val deleteSQL = config.getString("delete_sql")
    val database = config.getString("database")
    val table = config.getString("table")
    val partition = config.getInt("partition")
    println("start to saving data to mysql..")
    println(s"delete sql is $deleteSQL")
    //首先判断是否是需要删除历史数据。当saveMode为append的时候才需要考虑删除数据
    if (StringUtils.isNotBlank(deleteSQL)) {
      //当删除的sql不为空的时候
      val bool = validateTableExist(database, table)
      println(s"是否需要删除数据 $bool")
      if (bool) {
        val conn = getConnection()
        conn.setAutoCommit(false)
        val runner = new QueryRunner()
        try {
          runner.update(conn, deleteSQL)
          conn.commit()
          println(s"删除数据完成")

        } catch {
          case e => {
            e.printStackTrace()
            System.exit(1)
          }
        } finally {
          conn.close()
        }
      }
    }
    println(s"写入数据。。写入模式为: $saveMode")
    //删除数据后进行写入数据
    df.repartition(partition)
      .write
      .mode(saveMode)
      .jdbc(config.getString("url"), config.getString("table"), prop)
  }
}
