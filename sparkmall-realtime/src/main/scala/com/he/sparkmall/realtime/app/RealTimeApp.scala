package com.he.sparkmall.realtime.app

import com.he.sparkmall.common.util.MyKafkaUtil
import com.he.sparkmall.realtime.bean.AdsLog
import com.he.sparkmall.realtime.handler.{AreaCityAdsDayCountHandler, AreaTop3AdsCountHandler, BlacklistHandler, LastHourAdsCountHandler}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkMallRealTimeApp")
    val ssc = new StreamingContext(new SparkContext(sparkConf), Seconds(5))
//    ssc.checkpoint("./ck")
    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", ssc)
    val adsLogDS: DStream[AdsLog] = recordDstream.map { record =>
      val adsLogString: String = record.value()
      val adsLogArray: Array[String] = adsLogString.split(" ")
      AdsLog(adsLogArray(0).toLong, adsLogArray(1), adsLogArray(2), adsLogArray(3).toLong, adsLogArray(4).toLong)
    }
    //根据黑名单过滤
//    val filtedAdsLog: DStream[AdsLog] = BlacklistHandler.checkBlacklist(ssc.sparkContext,adsLogDS)
    //统计用户点击广告的次数，如果一天之内超过100，就将该用户加入黑名单。
    BlacklistHandler.updateUserAdsCount(adsLogDS)
    val dateAreaCityAdsCount: DStream[(String, Long)] = AreaCityAdsDayCountHandler.handle(adsLogDS,ssc)
    AreaTop3AdsCountHandler.handle(dateAreaCityAdsCount)
    LastHourAdsCountHandler.handle(adsLogDS)


    ssc.start()
    ssc.awaitTermination()
  }

}
