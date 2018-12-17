package com.mf.data_etl.config

/**
  * 维护生产库名到开发库名的映射
  */
object MongoDatabaseNameMapping {

  private lazy val mapping = Map(

    "gather" -> "gather"
  )

  def getDevName(prodName: String): String = mapping(prodName)

}
