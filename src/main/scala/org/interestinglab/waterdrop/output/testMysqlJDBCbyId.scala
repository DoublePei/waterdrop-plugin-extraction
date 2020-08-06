package org.interestinglab.waterdrop.output

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.commons.lang3.StringUtils

object testMysqlJDBCbyId {
  def main(args: Array[String]): Unit = {
    val bool = validateTableExist("test", "test")
    println(bool)
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

  def validateTableExist(db: String, tb: String): Boolean = {
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
