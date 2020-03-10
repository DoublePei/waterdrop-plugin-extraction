package org.interestinglab.waterdrop.output

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object test {
  def main(args: Array[String]): Unit = {

    if (StringUtils.isNotBlank("delete from test1 ")) {
      //当删除的sql不为空的时候
      val bool = validateTableExist("test1");
      println(s"是否需要删除数据 $bool")
      if (bool) {
        var conn: Connection = null
        var prepare: PreparedStatement = null
        try {
          conn = getConnection()
          conn.setAutoCommit(false)
          prepare = conn.prepareStatement("delete from test1 ");
          prepare.executeUpdate()
          conn.commit()
          println(s"删除数据完成")
        } catch {
          case e => {
            e.printStackTrace()
            System.exit(1)
          }
        } finally {
          if (conn != null) {
            conn.close()
          }
          if (prepare != null) {
            prepare.close()
          }
        }
      }
    }
  }

  def getConnection(): Connection = {
    var conn: Connection = null
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection("jdbc:mysql://47.93.254.72:3306/test"
        , "root", "root123")
    }
    conn
  }

  def validateTableExist(tb: String): Boolean = {
    var conn: Connection = null
    var flag = false
    try {
      conn = getConnection()
      val meta = conn.getMetaData
      val t = Array("TABLE")
      val rs = meta.getTables(null, null, tb, t)
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
}
