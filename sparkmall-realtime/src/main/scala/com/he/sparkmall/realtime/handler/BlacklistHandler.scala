package com.he.sparkmall.realtime.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.he.sparkmall.common.util.RedisUtil
import com.he.sparkmall.realtime.bean.AdsLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlacklistHandler {

  //更新用户点击广告的数量
  def updateUserAdsCount(adsLogDS: DStream[AdsLog]): Unit = {
    adsLogDS.foreachRDD { rdd =>
      rdd.foreachPartition { adsLogs =>
        val jedis: Jedis = RedisUtil.getJedisClient
        val format = new SimpleDateFormat("yyyy-MM-dd")
        val key = "UserAdsCount"
        for (adsLog <- adsLogs) {
          val uid: Long = adsLog.uid
          val date: String = format.format(new Date(adsLog.ts))
          val adsId: Long = adsLog.adsId
          val field = uid +":"+ date +":"+ adsId
          jedis.hincrBy(key, field, 1L)
          val count: Long = jedis.hget(key, field).toLong
          if (count > 100) {
            jedis.sadd("blacklist", uid.toString)
          }
        }
        jedis.close()
      }
    }
  }


  def checkBlacklist(sparkContext: SparkContext, adsLogDS: DStream[AdsLog]): DStream[AdsLog] = {
    val filtedDS: DStream[AdsLog] = adsLogDS.transform { rdd =>
      val jedis: Jedis = RedisUtil.getJedisClient
      //得到黑名单
      val blacklistSet: util.Set[String] = jedis.smembers("blacklist")
      //将黑名单作为广播变量
      val blacklistSetBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blacklistSet)
      //根据黑名单进行过滤
      val filtedRdd: RDD[AdsLog] = rdd.filter { adsLog =>
        val blacklistSet: util.Set[String] = blacklistSetBC.value
        !blacklistSet.contains(adsLog.uid.toString)
      }
      filtedRdd
    }
    filtedDS
  }


}
