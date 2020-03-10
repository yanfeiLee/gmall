package com.lyf.gmall.realtime.utils

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

object MyESUtil {

  private val ES_HOST = "http://hadoop104"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null


  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(
      new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
        .multiThreaded(true)
        .maxTotalConnection(20) //连接总数
        .connTimeout(10000)
        .readTimeout(10000)
        .build)
  }

  def getJestClient(): JestClient = {
    if (null == factory) build()
    factory.getObject
  }

  def close(client: JestClient) = {
    if (null != client) client.close()
  }

  //批量插入数据
  def insertBulk(indexName: String, docList: List[(String, Any)]) = {
    if (docList.size > 0) {
      //连接es
      val jest = getJestClient()

      //执行操作
      //设置操作的index和type
      val builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
      for ((id, doc) <- docList) {
        val indexBuilder = new Index.Builder(doc)
        if (id != null) indexBuilder.id(id)
        val index = indexBuilder.build()
        builder.addAction(index)
      }
      val bulk = builder.build()
      val result = jest.execute(bulk)


      //关闭连接
      close(jest)
      println("保存了" + result.getItems.size() + "条数据")
    }
  }


  def main(args: Array[String]): Unit = {

    //插入单条数据
    /*val index: Index = new Index.Builder(Stu("zhang3", "zhang33"))
      .index("gmall_stu")
      .`type`("_doc")
      .id("stu123")
      .build()
    val jest: JestClient = getJestClient
    jest.execute(index)
    close(jest)*/


    //批量插入数据
    insertBulk("gmall_stu",List(("stu1",Stu("aa","bbb")),("stu2",Stu("aa2","bbb2"))))

  }

}

case class Stu(name: String, nickName: String) {}