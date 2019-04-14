package com.he.sparkmall.realtime.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.he.sparkmall.common.util.RedisUtil
import com.he.sparkmall.realtime.bean.AdsLog
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.util.parsing.json.JSONObject

// key:last_hour_ads_click     field:11   value:{“8:30”:122, “8:31”:32, “8:32”:910, “8:33”:21 , ………}
object LastHourAdsCountHandler {
  def handle(filtedAdsLog: DStream[AdsLog]): Unit = {
    val lastHourAdsLog: DStream[AdsLog] = filtedAdsLog.window(Minutes(60), Seconds(10))
    val adsTimeToOneIterDS: DStream[(String, Long)] = lastHourAdsLog.mapPartitions { adsLogsIter =>
      val sdf = new SimpleDateFormat("HH:mm")
      val adsTimeToOneIter: Iterator[(String, Long)] = adsLogsIter.map { adsLog =>
        val time: String = sdf.format(new Date(adsLog.ts))
        val key = adsLog.adsId + "_" + time
        (key, 1L)
      }
      adsTimeToOneIter
    }
    val adsTimeCountDS: DStream[(String, Long)] = adsTimeToOneIterDS.reduceByKey(_ + _)
    val adsTimeCountGroupedByAds: DStream[(String, Iterable[(String, Long)])] = adsTimeCountDS.map { case (adsTime, count) =>
      val adsTimeArray: Array[String] = adsTime.split("_")
      val ads: String = adsTimeArray(0)
      val time: String = adsTimeArray(1)
      (ads, (time, count))
    }.groupByKey
    val adsTimeCountJson: DStream[(String, String)] = adsTimeCountGroupedByAds.map { case (ads, timeCountIter) =>
      val timeCountMap: Map[String, Long] = timeCountIter.toMap
      val timeCountJson: String = JSONObject(timeCountMap).toString()
      (ads, timeCountJson)
    }
    adsTimeCountJson.foreachRDD { rdd =>
      import collection.JavaConversions._
      val adsTimeCountJsonArray: Array[(String, String)] = rdd.collect
      val adsTimeCountJsonMap: Map[String, String] = adsTimeCountJsonArray.toMap

      val jedis: Jedis = RedisUtil.getJedisClient
      val str: String = adsTimeCountJsonMap.mkString("\n")
      println(str)
      jedis.hmset("last_hour_ads_click", adsTimeCountJsonMap)
      jedis.close()
    }
  }


}
