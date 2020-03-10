package com.lyf.gmall.realtime.beans

import java.text.SimpleDateFormat
import java.util.Date

case class EventInfo(
                      mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      `type`: String,
                      evid: String,
                      pgid: String,
                      npgid: String,
                      itemid: String,
                      var logDate: String,
                      var logHour: String,
                      var ts: Long
                    ) {
  private val date = new Date(ts)
  private val format = new SimpleDateFormat("yyyy-MM-dd HH")
  private val dateStr: String = format.format(date)
  private val dateArr: Array[String] = dateStr.split(" ")
  this.logDate = dateArr(0)
  this.logHour = dateArr(1)
  ts = System.currentTimeMillis();
}
