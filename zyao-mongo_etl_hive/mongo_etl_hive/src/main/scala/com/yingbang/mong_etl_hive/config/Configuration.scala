package com.yingbang.mong_etl_hive.config

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.{Level, Logger}

/**
  *
  */
class Configuration private(
                             private val json: String
                           ) extends Serializable {

  private val PARSED: JSONObject = JSON.parseObject(json)  // 解析命令行参数

  // 初始化系统配置
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  System.setProperty("spark.mongodb.keep_alive_ms", "30000")

  //mongo原始库名映射
  lazy val db_original: String = PARSED.getString("db_original")
  //mongo分析库名映射
  lazy val db_analyze: String = PARSED.getString("db_analyze")

  //是否在本地运行spark
  lazy val is_local: Boolean = PARSED.getBoolean("is_local")
  // "authSource=admin"
  private lazy val auth_source = PARSED.getString("auth_source")
  //mongo原始库连接
  private lazy val mongo_uri_original = PARSED.getString("mongo_uri_original")
  //mongo分析库连接
  private lazy val mongo_uri_analyze = PARSED.getString("mongo_uri_analyze")
  //原始库
  def getMongoOriginalUri(db: String, collection: String): String = mongo_uri_original + "/" + db + "." + collection
  def getOriginalMongoUri: String = mongo_uri_original
  //分析库
  def getMongoAnalyzeUri(db: String, collection: String): String = mongo_uri_analyze + "/" + db + "." + collection
  def getAnalyzeMongoUri: String = mongo_uri_analyze
}

object Configuration {

  def apply(json: String): Configuration = new Configuration(json)

}
