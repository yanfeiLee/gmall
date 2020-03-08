package com.lyf.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.lyf.gmall.common.constant.GmallConstant
import com.lyf.gmall.realtime.beans.OrderInfo
import com.lyf.gmall.realtime.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))


    //消费kafka数据，写入到phoenix

    val ids = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)

    //    val res = ids.map(_.value())
    //    res.print(100)
    val orderInfos = ids.map(record => {
      val jsonString = record.value()
      JSON.parseObject(jsonString, classOf[OrderInfo])
    })
    //订单信息表，添加用户下单日期字段





    //写入到phoenix
    import org.apache.phoenix.spark._
    orderInfos.foreachRDD(rdd =>
      rdd.saveToPhoenix(
        "GMALL_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration(),
        Some("hadoop104,hadoop105,hadoop106:2181")
      )
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
