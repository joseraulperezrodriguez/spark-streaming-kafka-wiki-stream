package com.wiki.big.callback

import org.apache.kafka.clients.producer.Callback
import com.wiki.big.net.HttpClient
import scala.concurrent.ExecutionContext
import org.apache.kafka.clients.producer.RecordMetadata
import org.json.JSONObject

class ProducerCallback(url:String)(implicit val taskExecutor: ExecutionContext) extends Callback {
  
    val client = new HttpClient(url)
    var time = System.currentTimeMillis()

    def onCompletion(arg0: RecordMetadata, arg1: Exception) {
      val current = System.currentTimeMillis()
      if(current - time >= 60000) {
          val obj = new JSONObject()
          obj.put("time", current);
          obj.put("offset", arg0.offset());
          obj.put("topic", arg0.topic());
          client.sendData(obj.toString());
          time = current;
      }
    }
  
}