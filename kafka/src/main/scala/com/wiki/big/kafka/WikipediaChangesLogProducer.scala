package com.wiki.big.kafka

import java.util.Properties
import java.io.FileInputStream
import javax.ws.rs.client.ClientBuilder
import org.glassfish.jersey.media.sse.SseFeature
import com.wiki.big.callback.ProducerCallback
import com.wiki.big.stream.ThreadPoolBuilder
import org.glassfish.jersey.media.sse.EventSource
import com.wiki.big.stream.ChangeEventListener
import org.apache.kafka.clients.producer.KafkaProducer

object WikipediaChangesLogProducer extends App {
  
  val STREAM_URL = "wiki.url"
  val API_URL = "wiki.api"

  val properties = new Properties()
  val is = new FileInputStream(args(0))
    //InputStream is = new FileInputStream("producer.props");
  properties.load(is);

  val url = properties.getProperty(STREAM_URL)
  val api = properties.getProperty(API_URL)

  val client = ClientBuilder.newBuilder().register(SseFeature.SERVER_SENT_EVENTS.getClass).build()
  val target = client.target(url)

  implicit val ExecutionContext = ThreadPoolBuilder.buildExecutionContext(5)

  val producerCallback = new ProducerCallback(api)

  val es = EventSource.target(target).build()
        
  val changeListenerAndProducer = new ChangeEventListener(new KafkaProducer[Int, String](properties),producerCallback, properties)
  es.register(changeListenerAndProducer);
  es.open();
        
  val waiting = new scala.collection.mutable.ArraySeq[Long](10)
        
  val initial = 5l*1000l;
  for(i <- 0 until waiting.length) waiting.update(i, initial * (i+1))
                
  var i = 0
  var last = System.currentTimeMillis()

  while(true) {
    if(!es.isOpen()) {
      if(System.currentTimeMillis() - last >= waiting(i)) {
        System.out.println("Error trying to open websocket... " + (System.currentTimeMillis() - last))
        es.open()
        last = System.currentTimeMillis()
        i = (i + 1) % waiting.length
      }
    }
  }
  
}