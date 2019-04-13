package com.he.sparkmall.offline.app


import com.alibaba.fastjson.{JSON, JSONObject}
import com.he.sparkmall.common.bean.UserVisitAction
import com.he.sparkmall.common.util.PropertiesUtil
import com.he.sparkmall.offline.bean.CategoryCount
import com.he.sparkmall.offline.handler.{PageConverRationHandler, Top10CategoryHandler, Top10CategoryTop10SessionHandler}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}



object OfflineApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Offline").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //      condition.params.json={startDate:"2018-11-01", \
    //      endDate:"2018-12-28", \
    //      startAge: "20", \
    //      endAge: "50", \
    //条件参数
    val conditionParam: JSONObject = JSON.parseObject(PropertiesUtil.load("conditions.properties").getProperty("condition.params.json"))

    //读取数据
    val ds: Dataset[UserVisitAction] = readUserVisitAction(sparkSession, conditionParam)
    //需求一：求出活跃度前十的category，并保存到mysql中，活跃度的规则是点击量、下单量、付款量依次排序。
    //val top10CategoryList: List[CategoryCount] = Top10CategoryHandler.handle(sparkSession,ds)
    //需求二：Top10 热门品类中 Top10 活跃 Session 统计
    //Top10CategoryTop10SessionHandler.handle(sparkSession,ds,top10CategoryList)
    //需求四：页面单跳转化率统计
    PageConverRationHandler.handle(sparkSession,ds,conditionParam)


  }

  //得到需要的数据
  def readUserVisitAction(sparkSession: SparkSession, conditionParam: JSONObject): Dataset[UserVisitAction] = {
    //解析过滤条件
    val startDate: String = conditionParam.getString("startDate")
    val endDate: String = conditionParam.getString("endDate")
    val startAge: String = conditionParam.getString("startAge")
    val endAge: String = conditionParam.getString("endAge")
    //拼装sql
    val builder = new StringBuilder("select uv.* from user_visit_action uv join user_info ui on uv.user_id = ui.user_id where 1=1")
    if (startDate != null && startDate.length > 0) {
      builder.append(s" and uv.date >= '$startDate'")
    }
    if (endDate != null && endDate.length > 0) {
      builder.append(s" and uv.date <= '$endDate'")
    }
    if (startAge != null && startAge.length > 0) {
      builder.append(s" and ui.age >= $startAge")
    }

    if (endAge != null && endAge.length > 0) {
      builder.append(s" and ui.age <= $endAge")
    }

    //查询结果，并转换为DataSet类型
    import sparkSession.implicits._
    sparkSession.sql(builder.mkString).as[UserVisitAction]
  }


}
