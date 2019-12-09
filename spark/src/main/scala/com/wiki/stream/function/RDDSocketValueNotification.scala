package com.wiki.stream.function

import org.apache.spark.rdd.RDD
import com.wiki.stream.net.HttpClient
import org.json.JSONObject
import scala.concurrent.ExecutionContext

object RDDSocketValueNotification {
  
  def notificationFuntion(endpoint: String, dataPath: String)(implicit taskExecutor: ExecutionContext): RDD[JSONObject] => Unit = {
    val socketEndPoint = endpoint + (if(endpoint.trim().endsWith("/")) "" else "/") + dataPath
    ((rdd: RDD[JSONObject]) => {
        
        rdd.foreachPartition(iterable => {
          
          val client = new HttpClient(socketEndPoint)
          val result = new JSONObject()

          result.put("count", iterable.size)
          result.put("list", iterable)
          client.sendData(result.toString())

    });
    
    })
  }
  
  
}