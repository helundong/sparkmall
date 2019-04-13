package com.he.sparkmall.offline.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

import scala.collection.immutable.HashMap

class GroupbyCityCountUDAF extends UserDefinedAggregateFunction {
  //输入参数的类型
  override def inputSchema: StructType = new StructType(Array(StructField("city_name", StringType)))

  /*缓冲区中值的数据类型
  * 第一个Long是总点击数
  * 第二个Map的key是城市名称，value是该城市的点击数
  * */
  override def bufferSchema: StructType = new StructType(Array(StructField("click_count", LongType), StructField("city_map", MapType(StringType, LongType))))

  //返回值数据类型
  override def dataType: DataType = StringType

  //对于相同的输入是否一直返回相同的输出
  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = new HashMap
  }

  //更新数据,总数加一，根据城市名给城市对应城市的值加一
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + 1L
    val cityName: String = input.getString(0)
    val map: collection.Map[String, Long] = buffer.getMap[String, Long](1)
    buffer(1) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
  }

  //合并不同分区的数据，最后得到的数据为buffer1中的数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //合并总点击数
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //取出buff1中的城市点击数map
    val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](1)
    //取出buff2中的城市点击数map
    val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](1)
    //合并城市点击数map
    buffer1(1) = map1.foldLeft(map2) { case (map2, (cityName, clickCount)) =>
      map2 + (cityName -> (map2.getOrElse(cityName, 0L) + clickCount))
    }
  }

  //返回结果,注意按照map中的点击数降序排序，取出前两个
  //格式：北京21.2%，天津13.2%，其他65.6%
  override def evaluate(buffer: Row): Any = {
    //得到总点击数
    val clickCount: Long = buffer.getLong(0)
    //按照map中的点击数降序排序，取出前两个
    var list: List[(String, Long)] = buffer.getMap[String, Long](1).toList.sortBy(_._2).reverse.take(2)
    list = list :+ ("其他",clickCount-list(0)._2 - list(1)._2)
    val sb = new StringBuilder
    for(item <- list){
      //
      val ratio: Double = Math.round( item._2.toDouble * 1000 / clickCount)/10D
      sb.append(item._1).append("点击数：").append(item._2).append(",").append("占比：").append(ratio).append("%").append(",")
    }
    sb.toString().dropRight(1)
  }
}
