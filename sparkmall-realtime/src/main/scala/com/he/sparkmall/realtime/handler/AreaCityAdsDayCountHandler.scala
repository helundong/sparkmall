package com.he.sparkmall.realtime.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.he.sparkmall.common.util.RedisUtil
import com.he.sparkmall.realtime.bean.AdsLog
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object AreaCityAdsDayCountHandler {
  def handle(filtedAdsLog: DStream[AdsLog],ssc:StreamingContext): DStream[(String, Long)] ={
    val dateAreaCityAdsPairsDS: DStream[(String, Long)] = filtedAdsLog.mapPartitions { adslogIter =>
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val sb = new StringBuilder

      val dateAreaCityAdsPairs: Iterator[(String, Long)] = adslogIter.map { adsLog =>
        val area: String = adsLog.area
        val city: String = adsLog.city
        val adsId: Long = adsLog.adsId
        val date: String = format.format(new Date(adsLog.ts))
        val dateAreaCityAds = sb.append(date).append(":").append(area).append(":")
          .append(city).append(":").append(adsId).toString()
        sb.clear()
        (dateAreaCityAds, 1L)
      }
      dateAreaCityAdsPairs


    }

    //使用updateStateByKey统计一天的点击量
      //使用updateStateByKey需要设置checkpoint
    ssc.sparkContext.setCheckpointDir("./ck")
    val dateAreaCityAdsCount: DStream[(String, Long)] = dateAreaCityAdsPairsDS.updateStateByKey[Long] {(current:Seq[Long],previous:Option[Long]) =>
      val currentCount: Long = current.foldLeft(0L)(_ + _)
      val previousCount: Long = previous.getOrElse(0)
      Some(currentCount + previousCount)
    }

    //将结果保存到redis
    dateAreaCityAdsCount.foreachRDD{rdd=>
      //date:area:city:ads	   field:  2018-11-26:华北:北京:5     value: 12001
      rdd.foreachPartition{dateAreaCityAdsCountIter=>
        val jedis: Jedis = RedisUtil.getJedisClient
        val key = "date:area:city:ads"
        import collection.JavaConversions._
        val values: Map[String, String] = dateAreaCityAdsCountIter.map{case(field,count)=>(field,count.toString)}.toMap
        jedis.hmset(key,values)
        jedis.close()
      }
    }
    dateAreaCityAdsCount
  }

}
