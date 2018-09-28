package com.wiki.big.kafka.calc

import com.wiki.big.net.HttpClient

class WikiNotifier(client: HttpClient, data: String) extends Runnable {
  
	def run() = client.sendData(data)			
  
}