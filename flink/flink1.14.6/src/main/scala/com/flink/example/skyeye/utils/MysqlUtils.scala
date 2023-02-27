package com.flink.example.skyeye.utils

import com.mysql.jdbc.Driver
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

class MysqlUtils {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[MysqlUtils])
  private var jdbcUrl: String = _
  private var user: String = _
  private var password: String = _
  private var connection: Connection = _
  private var ps: PreparedStatement = _
  private var rs: ResultSet = _


  def this(jdbcUrl: String, user: String, password: String) {
    this()
    this.jdbcUrl = jdbcUrl
    this.user = user
    this.password = password
  }

  def getConnection: Connection = {
    try {
      DriverManager.registerDriver(new Driver)
      connection = DriverManager.getConnection(jdbcUrl, user, password)
    } catch {
      case e: Exception =>
        LOG.error("数据库连接失败")
        e.printStackTrace()
    }
    connection
  }

  def executeQuery(sql: String): ResultSet = {
    try {
      connection = getConnection
      ps = connection.prepareStatement(sql)
      rs = ps.executeQuery
      return rs
    } catch {
      case e: Exception =>
        LOG.error("read message error from Mysql:{}", e)
    }
    null
  }

  def close(): Unit = {
    try {
      if (rs != null) rs.close()
      if (ps != null) ps.close()
      if (connection != null) connection.close()
    } catch {
      case e: Exception =>
        LOG.error("Mysql connect close error:{}", e)
    }
  }

}
