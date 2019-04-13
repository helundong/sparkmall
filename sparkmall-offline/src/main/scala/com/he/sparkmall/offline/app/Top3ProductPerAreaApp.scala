package com.he.sparkmall.offline.app

import java.util.Properties

import com.he.sparkmall.common.util.PropertiesUtil
import com.he.sparkmall.offline.udf.GroupbyCityCountUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Top3ProductPerAreaApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top3ProductPerArea")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val properties: Properties = PropertiesUtil.load("config.properties")
    sparkSession.udf.register("city_count",new GroupbyCityCountUDAF)
    sparkSession.sql("select area,click_product_id,city_name from user_visit_action ua join city_info ci on ua.city_id=ci.city_id and ua.click_product_id >0").createOrReplaceTempView("t1")
    sparkSession.sql("select area,click_product_id,count(*) clickcount,city_count(city_name) city_remark from t1 group by area,click_product_id").createOrReplaceTempView("t2")
    sparkSession.sql("select area, click_product_id, clickcount, city_remark,rank() over(partition by area sort by clickcount desc) rk from t2").createOrReplaceTempView("t3")
    sparkSession.sql("select area, product_id,product_name, clickcount,city_remark from t3 join product_info pi on t3.click_product_id=pi.product_id where rk <=3")
//      .show(false)
      .write.format("jdbc")
      .option("url",properties.getProperty("jdbc.url"))
      .option("user",properties.getProperty("jdbc.user"))
      .option("password",properties.getProperty("jdbc.password"))
      .option("dbtable","area_count_info")
      .mode(SaveMode.Append).save()


  }

}
