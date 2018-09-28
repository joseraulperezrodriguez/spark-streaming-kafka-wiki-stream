package com.wiki.big.stream

import org.glassfish.jersey.media.sse.EventListener
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.Properties
import com.wiki.big.callback.ProducerCallback
import org.glassfish.jersey.media.sse.InboundEvent
import org.json.JSONObject
import org.apache.kafka.clients.producer.ProducerRecord


class ChangeEventListener(producer:KafkaProducer[Int,String], callback:ProducerCallback, 
    properties:Properties) extends EventListener {

    var messageNum=0
    
    def onEvent(arg0: InboundEvent) {
  		val obj = new JSONObject(arg0.readData())
  		  .put("producer_time", System.currentTimeMillis())
  		
  		val modality = obj.getString("wiki")
  		
  		val modAns = if(modality.contains("mediawiki")) "wikimedia"
  		else if(modality.contains("wikidata"))"wikidata"
  		else if(modality.contains("wiktionary"))"wiktionary"
  		else if(modality.contains("commonswiki"))"wikicommons"
  		else if(modality.length() == 6)"wikipedia"
  		else "wikiother"
  		
  		obj.put("modality", modality)
  		messageNum += 1
  		producer.send(new ProducerRecord[Int, String](modality,messageNum,obj.toString()), callback)		
	}
  
}