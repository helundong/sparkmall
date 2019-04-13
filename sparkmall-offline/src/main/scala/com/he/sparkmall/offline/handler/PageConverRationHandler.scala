package com.he.sparkmall.offline.handler

import java.util.UUID

import com.alibaba.fastjson.JSONObject
import com.he.sparkmall.common.bean.UserVisitAction
import com.he.sparkmall.common.util.JDBCUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object PageConverRationHandler {
  def handle(sparkSession: SparkSession, ds: Dataset[UserVisitAction], conditionParam: JSONObject): Unit = {
    val rdd: RDD[UserVisitAction] = ds.rdd
    val pageFlow: String = conditionParam.getString("targetPageFlow")
    //得到要统计的页面
    val pageFlowArray: Array[String] = pageFlow.split(",")
    //得到页面点击数并回收到driver
    val pageCount: Array[(Long, Long)] = rdd.filter(userVisitAction =>
      pageFlowArray.contains(userVisitAction.page_id.toString)
    ).map(userVisitAction => (userVisitAction.page_id, 1L)).reduceByKey(_ + _).collect()
    //将页面点击数变为广播变量
    val pageCountBC: Broadcast[Array[(Long, Long)]] = sparkSession.sparkContext.broadcast(pageCount)

    //开始统计单跳
    //先得需要统计的单跳页面的数组
    //掐头
    val toPageArray: Array[String] = pageFlowArray.drop(1)
    //去尾
    val fromPageArray: Array[String] = pageFlowArray.dropRight(1)
    //单跳页面数组
    val pageJumpArray: Array[String] = fromPageArray.zip(toPageArray).map { case (fromPage, toPage) => s"$fromPage-$toPage" }
    val pageJumpArrayBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(pageJumpArray)

    val groupedUV: RDD[(String, Iterable[UserVisitAction])] = rdd.groupBy(userVisitAction => userVisitAction.session_id)
    val sessionPageJumpRdd: RDD[(String, List[String])] = groupedUV.mapValues { uvIter =>
      val sortedUV: List[UserVisitAction] = uvIter.toList.sortBy(_.action_time)
      var index: Int = 0
      val pageJumpList: List[String] = sortedUV.map { userVisitAction =>
        if (index == 0) {
         val pageJump = 0 + "-" + userVisitAction.page_id
          index+=1
          pageJump

        } else {
          val pageJump = sortedUV(index-1).page_id + "-" + userVisitAction.page_id
          index+=1
          pageJump
        }
      }
      pageJumpList
    }

    val pageJumpRecord: RDD[String] = sessionPageJumpRdd.flatMap { case (sessionId, pageJumpList) =>
      //过滤用户单跳页面，留下包含要求跳转率的记录
      pageJumpList.filter(pageJumpArrayBC.value.contains)
    }

    pageJumpRecord.collect().foreach(println)
    //得到用户单跳页面的统计
    val uvPageJumpCount: collection.Map[String, Long] = pageJumpRecord.map((_, 1)).countByKey()
    val pageConversionRatio: collection.Map[String, Double] = uvPageJumpCount.map { case (pageJump, count) =>
      val pageClickCount: Map[Long, Long] = pageCount.toMap
      val pageId: String = pageJump.split("-")(1)
      val sumCount: Long = pageClickCount.get(pageId.toLong).get
      val conversionRatio: Double = Math.round(count * 1000D / sumCount) / 10D
      (pageJump, conversionRatio)
    }

    //使用UUID作为taskID
    val taskId: String = UUID.randomUUID().toString
    //转换形式为JDBCUtil的参数
    val pageConversionRatioIter: Iterable[Array[Any]] = pageConversionRatio.map { case (pageJump, conversionRatio) =>
      Array(taskId,pageJump, conversionRatio)
    }
    //保存到数据库
    JDBCUtil.executeBatchUpdate("insert into page_convert_ratio values(?,?,?)",pageConversionRatioIter)



  }


}
