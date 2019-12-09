package com.wiki.stream.function

import org.apache.spark.rdd.RDD
import org.json._
import com.wiki.stream.net.HttpClient
import scala.concurrent.ExecutionContext

object RDDSocketPairNotification {

  def notificationFuntion(endpoint: String, dataPath: String)(implicit taskExecutor: ExecutionContext): RDD[(String, Int)] => Unit = {
      val socketEndPoint = endpoint + (if(endpoint.trim().endsWith("/")) "" else "/") + dataPath
      ((rdd: RDD[(String, Int)]) => {

        rdd.foreachPartition(iterable => {

          val client = new HttpClient(socketEndPoint)
          val result = new JSONObject()

          val list = for (node <- iterable) yield {
            val (key, value) = node
            val obj = new JSONObject()
            obj.put("key", key)
            obj.put("value", value)
            obj
          }

          result.put("count", list.size)
          result.put("list", list)

          client.sendData(result.toString())

        })

      })
  }

}