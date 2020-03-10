package com.lyf.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.lyf.gmall.common.constant.GmallConstant
import com.lyf.gmall.realtime.beans.{OrderDetail, OrderInfo, SaleInfo}
import com.lyf.gmall.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


object SaleApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //    获取流
    //订单流
    val orderStream = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)
      .map(cr => JSON.parseObject(cr.value(), classOf[OrderInfo]))
    //订单明细流
    val orderDetailStream = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL, ssc)
      .map(cr => JSON.parseObject(cr.value(), classOf[OrderDetail]))


    //    合并流
    //流转为k（order_id）-v结构，并用fullouterJoin合并
    val orderKV = orderStream.map(order => (order.id, order))
    val orderDetailKV = orderDetailStream.map(orderDetail => (orderDetail.order_id, orderDetail))
    //全外连接
    val orderOrDetailStream = orderKV.fullOuterJoin(orderDetailKV)
    // 解决流的数据延迟造成的join不完全问题

    val saleStream = orderOrDetailStream.flatMap {
      case (order_id, (orderInfoOpt, orderDetailOpt)) => {
        val saleInfos = new ListBuffer[SaleInfo]
        //从连接池获取连接
        val jedis = RedisUtil.getJedisClient
        if (orderInfoOpt != None) {
          val orderInfo = orderInfoOpt.get
          //1.1 1orderInfo，orderDetail都不为空,数据到达时，存在交集
          if (orderDetailOpt != None) {
            //取二者交集
            val saleInfo = new SaleInfo(orderInfo, orderDetailOpt.get)
            saleInfos += saleInfo
          }
          //1.2 orderInfo流，先到达，orderDetail流部分到达，将orderInfo写入缓存，等待剩余orderDetail
          //    到达，进行join，写入orderInfo 到redis

          // orderInfo 存如到redis, type:String    k(orderInfo:order_id） v(orderInfo)
          val key = "orderInfo:" + orderInfo.id
          val orderInfoString = JSON.toJSONString(orderInfo, new SerializeConfig(true))
          jedis.setex(key, 3600, orderInfoString)
          //1.3 orderInfo流，后到达，需要从缓存中读OrderDetail数据，进行join
          val orderDetailSet = jedis.smembers("orderDetail:" + orderInfo.id)
          if (orderDetailSet != null && orderDetailSet.size() > 0) {
            import collection.JavaConversions._
            for (orderDetailSet <- orderDetailSet) {
              val orderDetail = JSON.parseObject(orderDetailSet, classOf[OrderDetail])
              val saleInfo2 = new SaleInfo(orderInfo, orderDetail)
              saleInfos += saleInfo2
            }
          }
        } else {
          val orderDetail = orderDetailOpt.get
          //2.1 orderDetail流先到达，写入redis
          //orderDetail type:set  k(orderDetail:order_id)  v(orderDetail)
          val key = "orderDetail:" + orderDetail.order_id
          val orderDetailString = JSON.toJSONString(orderDetail, new SerializeConfig(true))
          jedis.sadd(key, orderDetailString)

          //2.2 orderDetail流后到达，从缓存中取orderInfo,进行join
          val orderString = jedis.get("orderInfo:" + orderDetail.order_id)
          if (orderString != null) {
            val orderInfo = JSON.parseObject(orderString, classOf[OrderInfo])
            val saleInfo3 = new SaleInfo(orderInfo, orderDetail)
            saleInfos += saleInfo3
          }
        }
        jedis.close()
        saleInfos
      }
    }

    saleStream.print(100)

    ssc.start()
    ssc.awaitTermination()
  }
}
