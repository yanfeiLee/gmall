package com.lyf.gmall.realtime.app

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSON
import com.lyf.gmall.common.constant.GmallConstant
import com.lyf.gmall.realtime.beans.StartUpLog
import com.lyf.gmall.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
object DauApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val ids: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    //      ids.map(_.value()).print(100)
    //1.转换json字符串为StartUpLog对象
    val logStream = ids.map(record => {
      val jsonString = record.value()
      JSON.parseObject(jsonString, classOf[StartUpLog])
    })

    //2.1. 每5s更新，当日用户访问清单set，过滤掉5s间 当日已访问的用户
    val filterStream = logStream.transform(rdd => {
      //每次从redis获取已访问mid
      val jedis = RedisUtil.getJedisClient
      //获取当日key
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val dayString = formatter.format(LocalDate.now())
      val visited = jedis.smembers("dau:" + dayString)
      jedis.close()
      //将已访问mid 进行广播
      val visitedSet = ssc.sparkContext.broadcast(visited)
      println("过滤前：" + rdd.count())
      //过滤掉已访问mid
      val res = rdd.filter(log => {
        val filterSet = visitedSet.value
        if (null != filterSet && filterSet.size() > 0 && filterSet.contains(log.mid)) {
          false //过滤
        } else {
          true
        }
      })
      println("过滤后：" + res.count())
      res
    })

    //2.2 去除 每5s内 重复登录的mid
    val finalStream = filterStream.map(log => (log.mid, log)).groupByKey().flatMap {
      case (mid, itr) => {
        if (1 == itr.size) itr.take(1)
        else itr.toList.sortBy(_.ts).take(1)
      }
    }

    //将就是结果进行cache ,防止分流时，重复计算
    finalStream.cache()

    //2.3.利用redis 中的set集合，对每日 每5s间 重复登录的设备进行去重
    finalStream.foreachRDD(rdd => {
      rdd.foreachPartition(logItr => {
        //连接redis,存储数据到set中，key:"dau:2020-01-01"
        val jedis = RedisUtil.getJedisClient

        while (logItr.hasNext) {
          val log = logItr.next()
          val key = "dau:" + log.logDate
          val mid = log.mid
          jedis.sadd(key, mid)
          jedis.expire(key, 24 * 3600) //每个key，保留24小时
        }
        jedis.close()
      })
    })

    //3.数据分流，写入到分析数据库phoenix
    finalStream.foreachRDD(rdd=>{
        rdd.saveToPhoenix("GMALL_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          new Configuration(),
          Some("hadoop104,hadoop105,hadoop106:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
