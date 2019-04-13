package com.he.sparkmall.mock.util

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomNum {

  def apply(fromNum:Int,toNum:Int): Int =  {
    fromNum+ new Random().nextInt(toNum-fromNum+1)
  }
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean) ={
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    // 用delimiter分割  canRepeat为false则不允许重复
    if(canRepeat){
      val buffer = new ListBuffer[Int]()
      while (buffer.size<amount){
        buffer += RandomNum(fromNum,toNum)
      }
      buffer.mkString(delimiter)
    }else{
      val hashSet = new mutable.HashSet[Int]()
      while(hashSet.size<amount) {
        hashSet += RandomNum(fromNum,toNum)
      }
      hashSet.mkString(delimiter)
    }

  }

}