package com.yingbang.mong_etl_hive.tinaji

import java.text.SimpleDateFormat
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Date, TimeZone}

import com.yingbang.mong_etl_hive.config.{Configuration, Constants}
import com.yingbang.mong_etl_hive.utils.CommonUtils
import org.apache.spark.sql.SparkSession

class TianJiOperator (
                       private val spark: SparkSession,
                       private val config: Configuration
                     ) extends Serializable {

  /**
    * 定义时间格式获取昨日凌晨和今日凌晨的时间
    */
  private val formatter = new SimpleDateFormat("yyyyMMdd")
  formatter.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
  val yesterday: String = formatter.format(Date.from(Instant.now().minus(1, ChronoUnit.DAYS)))
  val today: String = formatter.format(Date.from(Instant.now().minus(0, ChronoUnit.DAYS)))
  val todayTime = formatter.parse(today).getTime / 1000
  val yesterdayTime = formatter.parse(yesterday).getTime / 1000

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
    * 获取mongo天机运营商原始数据
    */
  lazy val tjOperatorOrigin = "tjOperatorOrigin"
  CommonUtils.loadFromMongodb(spark, config.getMongoOriginalUri(config.db_original, Constants.tb_tj_operator_origin)).createTempView(tjOperatorOrigin)

  //构建天机运营商数据临时表
  lazy val tjOperatorOriginTemp = "tjOperatorOriginTemp"
  val tianJiOperator = spark.sql(
    s"""
      SELECT mobile                        --本机手机号
       |   , user_id                       --用户ID
       |   , create_time                   --运营商认证时间
       |   , item.call_time                --通话时间
       |   , item.receive_phone            --对方通话号码
       |   , item.call_type                --呼叫类型
       |   , item.trade_time               --通话时长
       |FROM
       |  (
       |  SELECT mobile, user_id, create_time, item
       |  FROM
       |    (
       |    SELECT mobile, user_id, create_time, explode(teldata.items) item
       |    FROM
       |      (
       |      SELECT data_list.userdata.phone mobile, user_id, create_time, explode(data_list.teldata) teldata
       |      FROM
       |        (
       |        SELECT user_id, explode(data_list) data_list, create_time
       |        FROM
       |          (
       |          SELECT user_id, wd_api_mobilephone_getdatav2_response.data.data_list data_list, create_time
       |          FROM $tjOperatorOrigin
       |          )
       |        )
       |      )
       |    )
       |  )
      """.stripMargin
  ).createTempView(tjOperatorOriginTemp)

  lazy val call_record = "call_record"
  spark.sql(
    s"""
      |SELECT mobile, dial_type, time, peer_number, user_id, create_time, CAST(duration AS int) duration, type
      |FROM
      |  (
      |  SELECT mobile, dial_type, time, peer_number, user_id, create_time, duration, time_parse(type, create_time, time, unix_timestamp(time)) type
      |  FROM
      |    (
      |    SELECT mobile, dial_type, time, peer_number, user_id, create_time, duration, EXPLODE(type) type
      |    FROM
      |      (
      |      SELECT mobile, dial_type, time, peer_number, user_id, create_time, duration, SPLIT(type, ",") type
      |      FROM
      |        (
      |        SELECT mobile, dial_type, time, peer_number, user_id, create_time, duration
      |             , day_divide(unix_timestamp(create_time),unix_timestamp(time)) type
      |        FROM
      |          (
      |          SELECT mobile
      |               , trade_time duration
      |               , call_type dial_type
      |               , receive_phone peer_number
      |               , user_id
      |               , call_time time
      |               , create_time
      |          FROM $tjOperatorOriginTemp
      |          )
      |        )
      |      )
      |    )
      |  )
      |WHERE type != "0"
    """.stripMargin
  ).createTempView(call_record)

  lazy val consume_bill = "consume_bill"
  spark.sql(
    s"""
       |SELECT mobile                                   --本机电话号码
       |     , create_time                              --运营商认证时间
       |     , CAST(item.pay_fee as int) total_fee      --费用
       |     , item.month bill_month                    --月份
       |FROM
       |  (
       |  SELECT data_list.userdata.phone mobile, create_time, explode(data_list.billdata) item
       |  FROM
       |    (
       |    SELECT explode(data_list) data_list, create_time
       |    FROM
       |      (
       |      SELECT user_id, wd_api_mobilephone_getdatav2_response.data.data_list data_list, create_time
       |      FROM $tjOperatorOrigin
       |      )
       |    )
       |  )
      """.stripMargin
  ).createTempView(consume_bill)


  spark.sql("use yingbang")

  spark.sql(
    s"""
      |insert into table tianji_consume_bill partition(partition_time=$yesterday)
      |select mobile, create_time
      |, CASE WHEN ISNULL(total_fee) THEN 0 ELSE total_fee END total_fee
      |, bill_month
      |from $consume_bill
      |WHERE unix_timestamp(create_time) < $todayTime
    """.stripMargin
  )

  spark.sql(
    s"""
       |insert into table tianji_call_record partition(partition_time=$yesterday)
       |select DISTINCT mobile, create_time, dial_type, time, peer_number, user_id, duration, type
       |from $call_record
       |WHERE unix_timestamp(create_time) < $todayTime
    """.stripMargin
  )

  spark.catalog.dropTempView(tjOperatorOrigin)
  spark.catalog.dropTempView(call_record)
  spark.catalog.dropTempView(consume_bill)
  spark.catalog.dropTempView(tjOperatorOriginTemp)

}

object TianJiOperator {
  def apply(spark: SparkSession, config: Configuration): TianJiOperator = new TianJiOperator(spark, config)
}
