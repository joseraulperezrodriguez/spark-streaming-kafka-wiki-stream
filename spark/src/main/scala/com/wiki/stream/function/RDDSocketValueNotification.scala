package com.wiki.stream.function

import org.apache.spark.rdd.RDD
import com.wiki.stream.net.HttpClient
import org.json.JSONObject
import java.util.ArrayList
import scala.concurrent.ExecutionContext

object RDDSocketValueNotification {
  
  def notificationFuntion(endpoint: String, dataPath: String)(implicit taskExecutor: ExecutionContext): RDD[JSONObject] => Unit = {
    val socketEndPoint = endpoint + (if(endpoint.trim().endsWith("/")) "" else "/") + dataPath
    return ((rdd: RDD[JSONObject]) => {
        
        rdd.foreachPartition(iterable => {
          
    			val client = new HttpClient(socketEndPoint)    			
    			val result = new JSONObject()
    			
    			var count=0
    			val list = new ArrayList[JSONObject]()
    			for(node <- iterable) {
    				list.add(node)
    				count += 1
    			}
    			
    			result.put("count", count)
    			result.put("list", list)
    			client.sendData(result.toString())
			
		});
    
    })
  }
  
  
}