package com.yingbang.mong_etl_hive.utils

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.yingbang.mong_etl_hive.config.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *
  */
object CommonUtils {

  /**
    * 获取SparkSession实例
    *
    * @param config  系统配置对象
    * @param appName spark job的名字
    * @return SparkSession实例
    */
  def getSpark(config: Configuration, appName: String): SparkSession = {

    val conf = new SparkConf()

    val builder = SparkSession
      .builder()
      .appName(appName)
      .config(conf)

    if (config.is_local) {
      builder.master("local[*]")
      conf.set("spark.debug.maxToStringFields", "100000")
    }
    builder.enableHiveSupport().getOrCreate()
  }

  /**
    * 从mongodb加载数据到spark，返回一个DataFrame实例
    *
    * @param spark    SparkSession实例
    * @param mongoUri 访问mongodb时用到的uri，format：mongodb://user:pwd@host:port/db.collection?authSource=admin
    * @return DataFrame
    */
  def loadFromMongodb(spark: SparkSession, mongoUri: String, sampleSize: Int = 10000): DataFrame = {
    spark
      .read
      .format("com.mongodb.spark.sql")
      .option("spark.mongodb.input.uri", mongoUri)
      .option("spark.mongodb.input.sampleSize", sampleSize) // document size for spark to infer mongo schema automatic
      .load()

  }

  /**
    * 将DataFrame中的数据保存到Mongodb
    *
    * @param db     库名
    * @param coll   表名
    * @param df     DataFrame实例
    * @param config 系统配置对象
    * @param mode   在把数据写入mongodb的时候，对原表数据进行怎样的处理，覆盖？追加？
    */
  def saveDataFrameToMongo(
                            db: String,
                            coll: String,
                            df: DataFrame,
                            config: Configuration,
                            mode: SaveMode
                          ): Unit = {
    MongoSpark
      .save(
        df.write.mode(mode),
        WriteConfig(Map(
          "spark.mongodb.output.uri" -> config.getMongoOriginalUri(db, coll)
        ))
      )

  }

}
