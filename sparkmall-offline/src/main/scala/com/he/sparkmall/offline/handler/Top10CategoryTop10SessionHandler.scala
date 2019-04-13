package com.he.sparkmall.offline.handler

import java.util.UUID

import com.he.sparkmall.common.bean.UserVisitAction
import com.he.sparkmall.common.util.JDBCUtil
import com.he.sparkmall.offline.bean.CategoryCount
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object Top10CategoryTop10SessionHandler {

  //需求二：Top10 热门品类中 Top10 活跃 Session 统计
  def handle(sparkSession: SparkSession, ds: Dataset[UserVisitAction], top10CategoryList: List[CategoryCount]): Unit = {
    //取出top10CategoryID
    val top10Cids: List[String] = top10CategoryList.map(_.cid)
    val top10CidsBc: Broadcast[List[String]] = sparkSession.sparkContext.broadcast(top10Cids)
    //过滤原始数据，只取出包含top10category的数据
    val uv: Dataset[UserVisitAction] = ds.filter(userVisitAction => top10CidsBc.value.contains(userVisitAction.click_category_id.toString))
    //根据category session分组
    val uvrdd: RDD[UserVisitAction] = uv.rdd
    val categorySessionCount: RDD[((Long, String), Long)] = uvrdd.map { userVisitAction =>
      ((userVisitAction.click_category_id, userVisitAction.session_id), 1L)
    }.reduceByKey(_ + _)
    //转换数据结构(cid,sid),count) =>(cid,(sid,count)) 然后按cid分组
    val groupdCategorySessionCount: RDD[(Long, Iterable[(Long, (String, Long))])] = categorySessionCount.map { case ((cid, sid), count) => (cid, (sid, count)) }.groupBy(_._1)
    //组内排序并取出前十，然后压平,放入Array中
    val top10CategoryTop10SessionCount: RDD[(Long, (String, Long))] = groupdCategorySessionCount.flatMap {
      _._2.toList.sortBy { case (cid, (sid, count)) => count }.reverse.take(10)
    }
    //生成taskID
    val taskID: String = UUID.randomUUID().toString
    //拼装数据，得到最终结果
    val result: Array[Array[Any]] = top10CategoryTop10SessionCount.map{case (cid,(sid,count))=>Array(taskID,cid,sid,count)}.collect()
    //将结果写入MySql
    JDBCUtil.executeBatchUpdate("insert into  top10_session_per_top10_cid values(?,?,?,?)",result)

  }
}
