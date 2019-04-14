package com.he.sparkmall.realtime.handler

import com.he.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.util.parsing.json.JSONObject

object AreaTop3AdsCountHandler {
  //每天各地区 top3 热门广告
  def handle(dateAreaCityAdsCount: DStream[(String, Long)]): Unit ={
    val dateAreaAdsCount: DStream[(String, Long)] = dateAreaCityAdsCount.map { case (dateAreaCityAds, count) =>
      val dateAreaCityAdsArray: Array[String] = dateAreaCityAds.split(":")
      val date: String = dateAreaCityAdsArray(0)
      val area: String = dateAreaCityAdsArray(1)
      val ads: String = dateAreaCityAdsArray(3)
      (date + ":" + area + ":" + ads, count)
    }.reduceByKey(_ + _)
    val dateAreaAdsCountTup: DStream[(String, (String, (String, Long)))] = dateAreaAdsCount.map { case (dateAreaAds, count) =>
      val dateAreaAdsArray: Array[String] = dateAreaAds.split(":")
      val date: String = dateAreaAdsArray(0)
      val area: String = dateAreaAdsArray(1)
      val ads: String = dateAreaAdsArray(2)
      (date, (area, (ads, count)))
    }
    val dateAreaAdsCountTupGroupedByDate: DStream[(String, Iterable[(String, (String, Long))])] = dateAreaAdsCountTup.groupByKey
    val dateareaTop3AdsCountJsonMapDS: DStream[(String, Map[String, String])] = dateAreaAdsCountTupGroupedByDate.map { case (date, areaAdsCount) =>
      val areaAdsCountGroupedByArea: Map[String, Iterable[(String, (String, Long))]] = areaAdsCount.groupBy(_._1)
      val areaTop3AdsCountJsonMap: Map[String, String] = areaAdsCountGroupedByArea.map { case (area, areaAdsCountTupIter) =>
        val adsCountIter: Iterable[(String, Long)] = areaAdsCountTupIter.map(_._2)
        //按照广告点击量降序排序并取出前三
        val top3AdsCount: Map[String, Long] = adsCountIter.toList.sortWith(_._2 > _._2).take(3).toMap
        val top3AdsCountJson: String = JSONObject(top3AdsCount).toString
        (area, top3AdsCountJson)
      }
      (date, areaTop3AdsCountJsonMap)
    }
    dateareaTop3AdsCountJsonMapDS.foreachRDD{rdd=>
      val jedis: Jedis = RedisUtil.getJedisClient
      import collection.JavaConversions._
     rdd.collect.foreach{case (date,areaTop3AdsCountJsonMap)=>
         val key = s"top3_ads_per_day:$date"
         jedis.hmset(key,areaTop3AdsCountJsonMap)

     }
      jedis.close()

    }
  }

}
