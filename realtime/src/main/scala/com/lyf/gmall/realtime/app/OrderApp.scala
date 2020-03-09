package com.lyf.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.lyf.gmall.common.constant.GmallConstant
import com.lyf.gmall.realtime.beans.OrderInfo
import com.lyf.gmall.realtime.utils.{MyKafkaUtil, PhoenixUtil, PropertiesUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))


    //消费kafka数据，写入到phoenix

    val ids = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)

    //广播zk地址
    val properties = PropertiesUtil.load("config.properties")
    val zk_addr = ssc.sparkContext.broadcast(properties.getProperty("zk.addr"))


    //    val res = ids.map(_.value())
    //    res.print(100)
    val orderInfos = ids.map(record => {
      val jsonString = record.value()
      JSON.parseObject(jsonString, classOf[OrderInfo])
    })
    //订单信息表，添加用户下单日期字段
    //各批次间，增加是否首次下单字段
    val firstOrder = orderInfos.mapPartitions(orderItr => {
      val orderList = orderItr.toList
      if (orderList.size > 0) {
        val userIdStr = orderList.map(("'"+_.user_id+"'")).mkString(",")
        val list = PhoenixUtil.queryList("select * from GMALL_USER_STATE where user_id in(" + userIdStr + ")")
        val map = list.map(mp => (mp.get("USER_ID").asInstanceOf[String], mp.get("IF_FIRST_ORDER").asInstanceOf[String])).toMap
        for (elem <- orderList) {
          val res = map.getOrElse(elem.user_id, "0")
          if (res.equals("1")) {
            elem.if_first_order = "0"
          } else {
            elem.if_first_order = "1"
          }
        }
      }
      orderList.toIterator
    })
    //各批次内，增加是否首次下单字
    val resOrder = firstOrder.map(orderInfos => (orderInfos.user_id, orderInfos))
      .groupByKey()
      .flatMap({
        case (user_id, orderItr) => {
          val list = orderItr.toList
          if (list.size > 1) {
            val sortOrder = list.sortWith((o1, o2) => o1.create_time < o2.create_time)
            //将其它订单的首次下单置为0
            for (i <- 1 to sortOrder.size) {
              sortOrder(i).if_first_order = "0"
            }
          }
          list
        }
      })
    //分流写入，所以在此处做缓存
    resOrder.cache();


    //更新状态表
    resOrder.foreachRDD(rdd => {
      rdd.map(orderInfo => (orderInfo.user_id, orderInfo.if_first_order))
        .saveToPhoenix("GMALL_USER_STATE", Seq("USER_ID", "IF_FIRST_ORDER"),
          new Configuration(), Some(zk_addr.value))
    })

    //写入到phoenix
    resOrder.foreachRDD(rdd =>
      rdd.saveToPhoenix(
        "GMALL_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR", "IF_FIRST_ORDER"),
        new Configuration(),
        Some(zk_addr.value)
      )
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
