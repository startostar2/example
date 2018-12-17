package com.yingbang.mong_etl_hive

import com.yingbang.mong_etl_hive.config.{Configuration, Constants}
import com.yingbang.mong_etl_hive.moxie.MxOperator
import com.yingbang.mong_etl_hive.tinaji.TianJiOperator
import com.yingbang.mong_etl_hive.utils.{CommonUtils, UDFManager}

object Execute {

  System.setProperty("hadoop.home.dir", Constants.hadoop_execute)

  def main(args: Array[String]): Unit = {

    val config = Configuration(args(0))

    val spark = CommonUtils.getSpark(config, this.getClass.getSimpleName)

    UDFManager(spark)

//    MxOperator(spark, config)

    TianJiOperator(spark, config)

    spark.stop()

  }


}
