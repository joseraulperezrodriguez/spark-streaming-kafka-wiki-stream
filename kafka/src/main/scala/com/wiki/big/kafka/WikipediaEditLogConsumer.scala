package com.wiki.big.kafka

import java.util.Properties
import java.io.FileInputStream
import com.wiki.big.kafka.calc.WikiQueue
import com.wiki.big.kafka.calc.WikiSumarizer
import com.wiki.big.stream.ThreadPoolBuilder
import java.util.Timer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.ArrayList

object WikipediaEditLogConsumer extends App {
  
  val TOPICS = "wiki.topics"
  
  val properties = new Properties()
		//InputStream is = new FileInputStream(args[0]);
	val is = new FileInputStream("consumer.props")
	properties.load(is)
		
	val api_path = properties.getProperty("api.path")
	val queue_size = properties.getProperty("queue.size")
	val time_batch = properties.getProperty("time.batch")
		
	val capacity = queue_size.toInt
	val time = time_batch.toLong
		
	val queue = new WikiQueue(capacity)
  
  implicit val taskExecutor = ThreadPoolBuilder.buildExecutionContext(capacity)
		
	val sumarizer = new WikiSumarizer(queue, api_path)
		
	val timer = new Timer()		
	timer.schedule(sumarizer, time, time);

	val topics = properties.getProperty(TOPICS).split(",")
	
	val partitions = new ArrayList[TopicPartition]()	
	for(t <- topics) partitions.add(new TopicPartition(t, 0))
	
	val consumer = new KafkaConsumer[Int, String](properties)		
		//consumer.subscribe(ts);	
	consumer.assign(partitions);
	consumer.seekToEnd(partitions);
			
	while(true) {
		val data = consumer.poll(10*1000)
		val iterator = data.iterator()
		while(iterator.hasNext()) {
			queue.add(iterator.next().value())
			//System.out.println("Receive -> " + record.value());
		}
	}
  
}