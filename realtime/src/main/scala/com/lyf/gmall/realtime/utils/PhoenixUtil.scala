package com.lyf.gmall.realtime.utils

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import java.util
import java.util.Map

import scala.collection.mutable.ListBuffer


object PhoenixUtil {
  val properties = PropertiesUtil.load("config.properties")
  private val url: String = properties.getProperty("phoenix.jdbc.url")

  def queryList(sql: String): List[Map[String, Any]] = {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val resultList: ListBuffer[Map[String, Any]] = new ListBuffer[Map[String, Any]]()
    val conn: Connection = DriverManager.getConnection(url)
    val stat: Statement = conn.createStatement
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql)

    val md: ResultSetMetaData = rs.getMetaData
    while (rs.next) {
      val rowData: Map[String, Any] = new util.HashMap[String, Any]()
      for (i <- 1 to md.getColumnCount) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList += rowData
    }

    stat.close()
    conn.close()
    resultList.toList
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val list: List[Map[String, Any]] = queryList("select * from GMALL_USER_STATE where user_id in(2,87,13,81,118,89,119,94,23,72)")
    println(list)
    println(list.size)
    // Thread.sleep(1000)
  }

}