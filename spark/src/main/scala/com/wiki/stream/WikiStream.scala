package com.wiki.stream

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.hadoop.fs._
import java.util.Properties
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategy
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.wiki.stream.function.RDDSocketValueNotification
import org.json._
import com.wiki.stream.function.RDDSocketPairNotification

import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors


object WikiStream extends App {
      
    
  val KAFKA_BROKER = "kafka.broker.list" 
	val KAFKA_TOPICS = "kafka.topics"
	val SOCKET_ENDPOINT = "socket.endpoint"
	
	if (args.length < 2) {
			println("Usage: java property file with the following params:\n" +
					"kafka.broker.list=host1:port1,host1:port1,..,hostn:portn\n" + 
					"kafka.topics=topic1,topic2,...,topic3\n" +
					"socket.enpoint=http://localhost:8080/spark-streaming/\n" + 
					"time in milliseconds")

			System.exit(1)
	}
	
	val milliseconds= args(1).toLong
	val poolSize = 5
	
	implicit val taskExecutor = ThreadPoolBuilder.buildExecutionContext(poolSize)
	
	
	val sparkConfig = new SparkConf().setAppName("Kafka Wiki Direct Stream")

	val streamingContext = new StreamingContext(sparkConfig, Durations.milliseconds(milliseconds))
	
	
	val pt = new Path(args(0))
	val fs = FileSystem.get(streamingContext.sparkContext.hadoopConfiguration)
	val inputStream = fs.open(pt)
	
	val properties = new Properties()
	properties.load(inputStream)

	val brokers = properties.getProperty(KAFKA_BROKER)
	val topics = properties.getProperty(KAFKA_TOPICS)
	val	socketEndpoint = properties.getProperty(SOCKET_ENDPOINT)
	
	val	topicsArray = topics.split(",")
	val	kafkaParams = Map("metadata.broker.list" -> brokers)
	
	val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsArray, kafkaParams)
	val locationStrategy = LocationStrategies.PreferBrokers
	
	val messages = KafkaUtils.createDirectStream(
				streamingContext,
				locationStrategy,
				consumerStrategy)
				
	val modalityPath = "modality"		
	val typePath = "type"		
	val languagePath = "language"
	val dataPath = "data"
	
  val entries = messages.map(record => new JSONObject(record.value()))
  entries.foreachRDD(RDDSocketValueNotification.notificationFuntion(socketEndpoint, dataPath))
  
  val typeCount = entries.map(jsonObject => (jsonObject.getString("type"),1)).reduceByKey((a,b) => a+b)  
	typeCount.foreachRDD(RDDSocketPairNotification.notificationFuntion(socketEndpoint, typePath))
	
	val modalityCount = entries.map(jsonObject => (jsonObject.getString("modality"),1)).reduceByKey((a,b) => a+b)  
	modalityCount.foreachRDD(RDDSocketPairNotification.notificationFuntion(socketEndpoint, modalityPath))
	
	val languageCount = entries
				.map(jsonObject => {
						val modality = jsonObject.getString("modality")
						if(modality.equals("wikipedia"))  (jsonObject.getString("server_name").substring(0, 2), 1)
						else (modality, 0)									
				}).reduceByKey((i1, i2) => i1 + i2)
	languageCount.foreachRDD(RDDSocketPairNotification.notificationFuntion(socketEndpoint, languagePath))
	
	
	streamingContext.start()
	streamingContext.awaitTermination()
	
}

