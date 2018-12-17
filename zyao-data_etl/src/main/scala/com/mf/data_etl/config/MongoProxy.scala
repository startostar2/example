package com.mf.data_etl.config

import com.mf.data_etl.utils.CommonUtils
import com.mongodb.{MongoClient, MongoClientURI}
import com.yingbang.data_etl.utils.CommonUtils
import org.bson.Document

class MongoProxy private(
                          private val config: Configuration
                        ) extends Serializable {

  private lazy val mongo = new MongoClient(new MongoClientURI(config.getMongoUri))

  sys.addShutdownHook {
    mongo.close()
  }

  import scala.collection.JavaConverters._

  /**
    * 传入filter，删除匹配的文档
    *
    * @param db     库名
    * @param coll   表名
    * @param filter 要删除哪些文档
    * @return 删除的条数
    */
  def removeDocuments(
                       db: String,
                       coll: String,
                       filter: Document
                     ): Long = {

    mongo.getDatabase(CommonUtils.mapProdDevDbName(config.isDev, db)).getCollection(coll).deleteMany(filter).getDeletedCount

  }

  /**
    * 向mongodb插入一条记录
    *
    * @param db   库名
    * @param coll 表名
    * @param doc  一个Document实例
    */
  def insertOne(
                 db: String,
                 coll: String,
                 doc: Document
               ): Unit = {

    mongo.getDatabase(CommonUtils.mapProdDevDbName(config.isDev, db)).getCollection(coll).insertOne(doc)

  }

  /**
    * 向mongodb插入多条记录
    *
    * @param db   库名
    * @param coll 表名
    * @param docs 多个Document实例
    */
  def insertMany(
                  db: String, // 库名
                  coll: String, // 表名
                  docs: List[Document] // 多个Document实例
                ): Unit = {

    val collObj = mongo.getDatabase(CommonUtils.mapProdDevDbName(config.isDev, db)).getCollection(coll)

    if (docs.nonEmpty) {
      collObj.insertMany(docs.asJava)
    }

  }

  /**
    * 修改mongodb里面的一条记录
    *
    * @param db 库名
    * @param coll 表名
    * @param filter 过滤条件，找到目标记录
    * @param doc 新的记录
    */
  def updateOne(
                 db: String,
                 coll: String,
                 filter: Document,
                 doc: Document
               ): Unit = {

    val collObj = mongo.getDatabase(CommonUtils.mapProdDevDbName(config.isDev, db)).getCollection(coll)

    collObj.updateOne(filter, doc)

  }

  /**
    * 覆写一条记录，根据过滤条件找到唯一的一条记录，先删除再插入新的数据
    *
    * @param db 库名
    * @param coll 表名
    * @param filter 过滤条件
    * @param newData 新的数据
    */
  def overwriteOne(
                    db: String,
                    coll: String,
                    filter: Document,
                    newData: Document
                  ): Unit = {

    val dbx = CommonUtils.mapProdDevDbName(config.isDev, db)

    removeDocuments(dbx, coll, filter)

    insertOne(dbx, coll, newData)

  }

}

object MongoProxy {

  private var instance: MongoProxy = _

  def getInstance(config: Configuration): MongoProxy = {
    if (this.instance == null) {
      this.synchronized {
        if (this.instance == null) {
          this.instance = new MongoProxy(config)
        }
      }
    }
    this.instance
  }

}
