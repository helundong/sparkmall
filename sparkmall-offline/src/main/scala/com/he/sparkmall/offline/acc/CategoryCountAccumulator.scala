package com.he.sparkmall.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryCountAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{
  private var map = new mutable.HashMap[String,Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val newAccumulator = new CategoryCountAccumulator
    newAccumulator.map ++= map
    newAccumulator
  }

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v,0L) + 1L
  }


  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val otherMap: mutable.HashMap[String, Long] = other.value
    map = map.foldLeft(otherMap){case (otherMap,(key,count)) =>
      otherMap(key) = otherMap.getOrElse(key,0L) + count
      otherMap
    }
  }

  override def value: mutable.HashMap[String, Long] = this.map
}
