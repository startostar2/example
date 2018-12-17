package com.mf.data_etl.config

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mf.data_etl.utils.CommonUtils
import org.apache.log4j.{Level, Logger}

/**
  * Crate by 2018/3/30 ZY 15:55
  *
  */
case class Configuration(
                          private val json: String
                        ) extends Serializable {

  private val PARSED: JSONObject = JSON.parseObject(json) // 解析命令行参数

  // 初始化系统配置
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  System.setProperty("spark.mongodb.keep_alive_ms", "30000")

  /**
    * 是否在本地运行spark
    */
  def isLocal: lang.Boolean = PARSED.getBoolean("is_local")

  /**
    * 获取MongoDB的uri
    */
  def getMongoUri(database: String, table: String): String = {
    val mongo = new StringBuilder

    mongo.append(getBaseMongoUri)
      .append("/")
      .append(CommonUtils.mapProdDevDbName(isDev, database))
      .append(".")
      .append(table)

    mongo.toString()
  }

  /**
    * 获取MongoDB的uri，不指定库表
    */
  def getMongoUri: String = getBaseMongoUri

  private def getBaseMongoUri = if (isDev) Constants.mongo_uri_base_dev else Constants.mongo_uri_base_prod

  /**
    * 是否是生产环境
    */
  def isDev: lang.Boolean = PARSED.getBoolean("is_dev")

}
