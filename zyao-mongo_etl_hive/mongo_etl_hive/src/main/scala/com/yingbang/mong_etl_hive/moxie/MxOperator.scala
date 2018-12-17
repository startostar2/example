package com.yingbang.mong_etl_hive.moxie

import java.text.SimpleDateFormat
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Date, TimeZone}

import com.yingbang.mong_etl_hive.config.{Configuration, Constants}
import com.yingbang.mong_etl_hive.utils.CommonUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class MxOperator(
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
    * 获取mongo魔蝎运营商原始数据
    */
  lazy val mxOperatorOrigin = "mxOperatorOrigin"
  CommonUtils.loadFromMongodb(spark, config.getMongoOriginalUri(config.db_original, Constants.tb_mx_operator_origin)).createTempView(mxOperatorOrigin)
  //获取用户信息
  lazy val realName = "realName"
  CommonUtils.loadFromMongodb(spark, config.getMongoAnalyzeUri(config.db_analyze, Constants.tb_real_name)).createTempView(realName)
  //获取通讯录名单
  lazy val contactsInfo = "contactsInfo"
  CommonUtils.loadFromMongodb(spark, config.getMongoAnalyzeUri(config.db_analyze, Constants.tb_contacts_info)).createTempView(contactsInfo)

  //构建魔蝎运营商数据临时表
  lazy val mxOperatorOriginTemp = "mxOperatorOriginTemp"
  val moxieOperator = spark.sql(
    s"""
       |SELECT mobile               --本机电话号码
       |     , items.peer_number    --对方号码
       |     , items.details_id     --通话详情ID
       |     , items.dial_type      --通话类型
       |     , items.duration       --通话时长秒
       |     , items.time           --通话时间
       |     , create_time          --运营商认证时间
       |     , user_id              --用户ID
       |FROM
       |  (
       |  SELECT mobile, user_id, items, create_time, explode(bills) bills
       |  FROM
       |    (
       |    SELECT mobile, user_id, explode(item) items, create_time, bills
       |    FROM
       |      (
       |      SELECT mobile, user_id, explode(items) item, create_time, bills
       |      FROM
       |        (
       |        SELECT mobile, user_id, calls.items, create_time, bills
       |        FROM $mxOperatorOrigin
       |        )
       |      )
       |    )
       |  )
      """.stripMargin
  ).createTempView(mxOperatorOriginTemp)

  /**
    * 获取 30, 60, 90, 150 不同类型通话记录
    */
  lazy val call_record = "call_record"
  spark.sql(
    s"""
       |SELECT mobile, dial_type, time, peer_number, user_id, create_time, details_id, duration, type
       |FROM
       |  (
       |  SELECT mobile, dial_type, time, peer_number, user_id, create_time, details_id, duration, time_parse(type, create_time, time, unix_timestamp(time)) type
       |  FROM
       |    (
       |    SELECT mobile, dial_type, time, peer_number, user_id, create_time, details_id, duration, EXPLODE(type) type
       |    FROM
       |      (
       |      SELECT mobile, dial_type, time, peer_number, user_id, create_time, details_id, duration, SPLIT(type, ",") type
       |      FROM
       |        (
       |        SELECT mobile, dial_type, time, peer_number, user_id, create_time, details_id, duration
       |             , day_divide(unix_timestamp(create_time),unix_timestamp(time)) type
       |        FROM
       |          (
       |          SELECT mobile, details_id
       |               , duration, dial_type
       |               , peer_number, user_id
       |               , time
       |               , create_time
       |          FROM $mxOperatorOriginTemp
       |          )
       |        )
       |      )
       |    )
       |  )
       |WHERE type != "0"
      """.stripMargin
  ).createTempView(call_record)

  //魔蝎通讯录账单
  lazy val consume_bill = "consume_bill"
  val consumeFrame = spark.sql(
    s"""
       |SELECT mobile               --本机电话号码
       |     , create_time          --运营商认证时间
       |     , bills.total_fee      --费用
       |     , bills.bill_month     --月份
       |FROM
       |  (
       |  SELECT mobile, create_time, explode(bills) bills
       |  FROM
       |    (
       |    SELECT mobile, create_time, bills
       |    FROM $mxOperatorOrigin
       |    )
       |  )
      """.stripMargin
  ).createTempView(consume_bill)

  spark.sql("use yingbang")

  spark.sql(
    s"""
       |insert into table contacts_info partition(partition_time=$yesterday)
       |SELECT userId, phone
       |FROM $contactsInfo
       |WHERE unix_timestamp(gmtCreate) < $todayTime
       |AND unix_timestamp(gmtCreate) > $yesterdayTime
    """.stripMargin
  )

  spark.sql(
    s"""
       |insert overwrite table real_name
       |SELECT phone, userId
       |FROM $realName
    """.stripMargin
  )

  spark.sql(
    s"""
       |insert into table moxie_consume_bill partition(partition_time=$yesterday)
       |select mobile, create_time, total_fee, bill_month
       |from $consume_bill
       |WHERE unix_timestamp(create_time) < $todayTime
       |AND unix_timestamp(create_time) > $yesterdayTime
    """.stripMargin
  )
  spark.sql(
    s"""
       |insert into table moxie_call_record partition(partition_time=$yesterday)
       |select DISTINCT mobile, create_time, dial_type, time, peer_number, user_id, details_id, duration, type
       |from $call_record
       |WHERE unix_timestamp(create_time) < $todayTime
       |AND unix_timestamp(create_time) > $yesterdayTime
    """.stripMargin
  )

  //删除临时表
  spark.catalog.dropTempView(mxOperatorOriginTemp)
  spark.catalog.dropTempView(mxOperatorOrigin)
  spark.catalog.dropTempView(realName)
  spark.catalog.dropTempView(contactsInfo)
  spark.catalog.dropTempView(consume_bill)
  spark.catalog.dropTempView(call_record)


}

object MxOperator {
  def apply(spark: SparkSession, config: Configuration): MxOperator = new MxOperator(spark, config)
}
