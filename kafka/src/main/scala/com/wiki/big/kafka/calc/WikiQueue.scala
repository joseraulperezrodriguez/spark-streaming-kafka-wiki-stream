package com.wiki.big.kafka.calc

import java.util.concurrent.atomic.AtomicInteger

class WikiQueue(capacity: Int) {
  
  val queue = new scala.collection.mutable.ArraySeq[String](capacity)
	val size = new AtomicInteger(0)
  var head = 0
	var tail = 0
	val	popList = scala.collection.mutable.ListBuffer[String]()
	
	def add(data: String): Boolean = {
		if(size.get() < queue.length) {
			queue.update(tail,data)
			size.incrementAndGet()
			tail = (tail + 1) % queue.length
			true
		} else false		
	}
	
	def pop(): scala.collection.mutable.ListBuffer[String] = {
		val cs = size.get()
		
		popList.clear()
		
		for(i <- 0 until cs) {
			val data = queue(((head + i) % queue.length))			
			popList.append(data)
		}
		
		head = (head + cs) % queue.length
		size.getAndAdd(cs * (-1))
		
		popList
	}
  
}