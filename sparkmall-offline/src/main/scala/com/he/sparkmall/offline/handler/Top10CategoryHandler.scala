package com.he.sparkmall.offline.handler

import java.util.UUID

import com.he.sparkmall.common.bean.UserVisitAction
import com.he.sparkmall.common.util.JDBCUtil
import com.he.sparkmall.offline.acc.CategoryCountAccumulator
import com.he.sparkmall.offline.bean.CategoryCount
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.{immutable, mutable}

//取出前十并保存到MySql
object Top10CategoryHandler {
  def handle(sparkSession: SparkSession,ds: Dataset[UserVisitAction]): List[CategoryCount] ={
    //创建累加器对象
    val categoryCountAccumulator = new CategoryCountAccumulator
    //注册累加器
    sparkSession.sparkContext.register(categoryCountAccumulator)
    //遍历对象ds，统计各个指标的数据。
    ds.foreach { userVisitAction =>
      //统计类目点击量
      if (userVisitAction.click_category_id != -1){
        categoryCountAccumulator.add(s"${userVisitAction.click_category_id}_click")
        //统计类目下单量
      }else if(userVisitAction.order_category_ids != null){
        val cidArray: Array[String] = userVisitAction.order_category_ids.split(",")
        for(id <- cidArray){
          categoryCountAccumulator.add(s"${id}_order")
        }
        //统计类目付款量
      }else if(userVisitAction.pay_category_ids != null){
        val cidArray: Array[String] = userVisitAction.pay_category_ids.split(",")
        for(id <- cidArray){
          categoryCountAccumulator.add(s"${id}_pay")
        }
      }
    }

    //得到统计的结果
    val result: mutable.HashMap[String, Long] = categoryCountAccumulator.value
    println(result.mkString("\n"))
    //取出前十
    //1.排序，先按cid分组，然后将每组的数据拼装为一个bean，然后排序
    //按cid分组
    val cidMap: Map[String, mutable.HashMap[String, Long]] = result.groupBy { case (key, value) =>
      key.split("_")(0)
    }

    //创建taskId
    val taskId: String = UUID.randomUUID().toString
    //拼装bean
    val categoryCounts: immutable.Iterable[CategoryCount] = cidMap.map { case (cid, map) =>
      CategoryCount(taskId, cid, map.getOrElse(cid + "_click", 0), map.getOrElse(cid + "_order", 0), map.getOrElse(cid + "_pay", 0))
    }
    //排序
    val sortedCategoryCount: List[CategoryCount] = categoryCounts.toList.sortWith { (c1, c2) =>
      //先根据点击量比较
      if (c1.clickCount > c2.clickCount) {
        true
      } else if (c1.clickCount == c2.clickCount) {
        //如果点击量相等使用订单量比较
        if (c1.orderCount > c2.orderCount) {
          true
          //如果订单量还相等就用付款量比较
        } else if (c1.payCount > c2.payCount) {
          true
        } else {
          false
        }
      } else {
        false
      }
    }
    //2.取出前十
    val top10List: List[CategoryCount] = sortedCategoryCount.take(10)
    println(top10List)
    //3.将前十存入MySql,先将数据转换为JDBCUtil支持的Array
    //Array("task1","123",100,200, 34 )
    val list: List[Array[Any]] = top10List.map { categoryCount =>
      Array(categoryCount.taskId, categoryCount.cid, categoryCount.clickCount, categoryCount.orderCount, categoryCount.payCount)
    }
    JDBCUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)",list)
    top10List
  }

}
