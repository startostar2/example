package com.mf.data_etl.config

/**
  * 维护系统所需常量
  */
object Constants {
  lazy val hadoop_execute = "D:\\install\\hadoop-common-2.6.0-bin\\hadoop-common-2.6.0-bin-master"

  lazy val mongo_uri_base_dev = "mongodb://"
  lazy val mongo_uri_base_prod = "mongodb://dds-bp1c30e6691173a42508-pub.mongodb.rds.aliyuncs.com:3717"
  lazy val mongo_auth_source = "admin"

  lazy val gather = "gather"

  lazy val tb_mx_operator_origin = "mx_operator_origin"

}
