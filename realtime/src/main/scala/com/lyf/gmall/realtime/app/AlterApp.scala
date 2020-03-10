package com.lyf.gmall.realtime.app

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util

import com.alibaba.fastjson.JSON
import com.lyf.gmall.common.constant.GmallConstant
import com.lyf.gmall.realtime.beans.{AlterInfo, EventInfo}
import com.lyf.gmall.realtime.utils.{MyESUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object AlterApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlterApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val ids = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT, ssc)
    //5min内
    val ds = ids.map(_.value()).window(Minutes(5), Seconds(5))

    //同一设备
    val eventGroupStream = ds.map(cr => {
      val eventInfo = JSON.parseObject(cr, classOf[EventInfo])
      (eventInfo.mid, eventInfo)
    }).groupByKey()

    //三个不同账号登录，领取优惠券，没有浏览商品
    val eventStream = eventGroupStream.map {
      case (mid, eventInfoItr) => {
        val uidSet = new util.HashSet[String]()
        val itemSet = new util.HashSet[String]()
        val eventList = new util.ArrayList[String]()
        var ifVisitItem = false
        import scala.util.control.Breaks._
        breakable(
          for (eventInfo <- eventInfoItr) {
            eventList.add(eventInfo.evid)
            if (eventInfo.evid == "coupon") {
              uidSet.add(eventInfo.uid)
              itemSet.add(eventInfo.itemid)
            }
            if (eventInfo.evid == "clickItem") {
              ifVisitItem = true
              break
            }
          }
        )
        //生成预警日志

        //三个不同账号登录，且没有浏览商品
        val res = !ifVisitItem && uidSet.size() >= 3
        (res, AlterInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
      }
    }
    val alterInfoStream = eventStream.filter(_._1)
      .map {
        case (isAlert, alertInfo) => (alertInfo.mid, alertInfo)
      }


    //同一设备每分钟，只记录一次预警(去重)
    //利用es进行去重,利用mid+分钟为id,进行去重，确保同一设备每分钟，只记录一次预警
    alterInfoStream.foreachRDD(rdd => {
      rdd.foreachPartition(alertInfoItr => {
        val alertItr = alertInfoItr.map{case(mid,alertInfo)=>(mid+"_"+alertInfo.ts/1000/60,alertInfo)}
        val alertInfoList = alertItr.toList
        //按每日创建索引 同时在es中创建模板以便于查询（es-dsl.txt）
        val day = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now())
        MyESUtil.insertBulk(GmallConstant.ES_INDEX_GMALL_COUPON_ALERT+"_"+day, alertInfoList)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
