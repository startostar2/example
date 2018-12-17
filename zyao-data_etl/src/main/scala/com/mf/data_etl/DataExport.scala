package com.mf.data_etl

import com.mf.data_etl.config.{Configuration, Constants}
import com.mf.data_etl.utils.CommonUtils


object DataExport {

  System.setProperty("hadoop.home.dir",Constants.hadoop_execute )

  def main(args: Array[String]): Unit = {


    val config = Configuration(args(0))

    val spark = CommonUtils.getSpark(config, this.getClass.getSimpleName)


    val mxOperatorOrigin = "mxOperatorOrigin"

//    CommonUtils.loadFromMongodb(spark, config.getMongoUri(Constants.gather, Constants.tb_mx_operator_origin)).createTempView(mxOperatorOrigin)

    spark.sql(
      s"""
        |SELECT count(item.details_id) count
        |FROM
        |  (
        |  SELECT mobile, explode(calls.items) item
        |  FROM $mxOperatorOrigin
        |  )
        |
      """.stripMargin
    ).show(false)
  }

}
