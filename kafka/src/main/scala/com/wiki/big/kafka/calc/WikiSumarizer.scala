package com.wiki.big.kafka.calc

import java.util.TimerTask
import scala.concurrent.ExecutionContext
import com.wiki.big.net.HttpClient
import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable.HashMap
import org.json.JSONObject
import java.util.ArrayList

class WikiSumarizer(queue: WikiQueue, api: String)(implicit val taskExecutor: ExecutionContext) 
extends TimerTask {

	val clientData = new HttpClient(api + "data")
	val clientType = new HttpClient(api + "type")
	val	clientModality = new HttpClient(api + "modality")
	val	clientLanguage = new HttpClient(api + "language")

	val blockingQueue = new ArrayBlockingQueue[Runnable](100)

	//val threadPoolExecutor = new ThreadPoolExecutor(5, 10, 4000, TimeUnit.MILLISECONDS, blockingQueue);
	val typeCount = new HashMap[String, Int]()		
	val modalityCount = new HashMap[String, Int]()
	val languageCount = new HashMap[String, Int]()
	
	def run() {
		
		val tuples = queue.pop()
		
		typeCount.clear()
		modalityCount.clear()
		languageCount.clear()
		
		val listValue = new ArrayList[JSONObject]()
		
		for(str <- tuples) {
				val obj = new JSONObject(str)
				listValue.add(obj)
				
				val `type` = obj.getString("type")
				val modality = obj.getString("modality")
				
				val lang =  
				  if(modality.equals("wikipedia") || modality.equals("wiktionary")) 
					  obj.getString("server_name").substring(0, 2)										
					else modality
				 				
				updateCount(`type`, typeCount)
				updateCount(modality, modalityCount)
				updateCount(lang, languageCount)				
		}		
		
		val data = new JSONObject()
		data.put("count", listValue.size())
		data.put("list", listValue)
			
		val `type` = prepareObject(typeCount)
		val modality = prepareObject(modalityCount)
		val language = prepareObject(languageCount)			
			
		taskExecutor.execute(new WikiNotifier(clientData, data.toString()))			
		taskExecutor.execute(new WikiNotifier(clientType, `type`.toString()))
		taskExecutor.execute(new WikiNotifier(clientModality, modality.toString()))
		taskExecutor.execute(new WikiNotifier(clientLanguage, language.toString()))
							
	}
	

	def prepareObject(map:HashMap[String, Int]) : JSONObject = {
		val `type` = new JSONObject()
		`type`.put("count", map.size)
		val list = new ArrayList[JSONObject]()
		for(entry <- map) {
			val obj = new JSONObject()
			obj.put("key", entry._1)
			obj.put("value", entry._2)			
			list.add(obj)
		}
		`type`.put("list", list)
		`type`
	}

	def updateCount(value:String, map:HashMap[String, Int]) {
		if(!value.isEmpty()) {
			var count = map.get(value)
			var countAns =
			    if(!count.isDefined) 0
			    else count.get 
			map.put(value, countAns + 1)
		}
	}

}