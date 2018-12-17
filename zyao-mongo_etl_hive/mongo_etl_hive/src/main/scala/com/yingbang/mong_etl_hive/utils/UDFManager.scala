package com.yingbang.mong_etl_hive.utils

import java.sql.Date
import java.text.SimpleDateFormat
import java.time.LocalDate

import org.apache.spark.sql.SparkSession

/**
  * Manage udf of spark
  * spark 自定义函数
  */
class UDFManager(spark: SparkSession) extends Serializable {

  val simpleDateFormat = new SimpleDateFormat("yyyy-MM")

  //定义时间格式1分钟,单位为秒
  val minute = 60
  //定义时间格式为30天, 单位为秒
  lazy val thirty = 2592000
  //定义时间格式为60天, 单位为秒
  lazy val sixty = 5184000
  //定义时间格式为90天, 单位为秒
  lazy val ninety = 7776000
  //定义时间格式为180天, 单位为秒
  lazy val hundredEighty = 15552000

  /**
    * 获取昨天时间
    */
  spark.udf.register("yesterday", () => Date.valueOf(LocalDate.now().minusDays(1)))

  /**
    * 自定义UDF 解析时间区间,划分为四个区间
    */
  spark.udf.register("day_divide", (create_time: Long, time:Long) => {
    if(create_time - thirty < time && time < create_time){
      "30,60,90,180"
    }else if(create_time - sixty < time && time < create_time){
      "60,90,180"
    }else if(create_time - ninety < time && time < create_time){
      "90,180"
    }else if(create_time - hundredEighty < time && time < create_time){
      "180"
    }else{
      "0"
    }
    })

  /**
    * 自定义解析时间格式
    */
  spark.udf.register("time_parse",(types:String, create_time:String, call_time: String, time:Long)=>{

    if("180" == types){

      val ctime = create_time.substring(0,7)
      val ctimeDate = simpleDateFormat.parse(ctime)

      val callTime = call_time.substring(0,7)
      val timeDate = simpleDateFormat.parse(callTime)

      val createTime = ctimeDate.getTime/1000
      val callTimes = timeDate.getTime/1000

      val middle = createTime - callTimes

      if(createTime - middle < time && time < createTime){
        "150"
      }else{
        "0"
      }
    }else if ("30" == types){
      "30"
    }else if ("60" == types){
      "60"
    }else if ("90" == types){
      "90"
    }else{
      "-1"
    }

  })



}

object UDFManager {

  def apply(spark: SparkSession): UDFManager = new UDFManager(spark)

}