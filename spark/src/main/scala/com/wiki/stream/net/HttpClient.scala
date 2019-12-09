package com.wiki.stream.net

import java.net.HttpURLConnection
import java.net.URL
import java.io.DataOutputStream
import scala.concurrent.ExecutionContext

class HttpClient(url: String)(implicit val taskExecutor: ExecutionContext) {
  
  val urlR = new URL(url)
  
  def sendData(json: String) {
      val connection = urlR.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setRequestProperty("Content-Length", Integer.toString(json.getBytes().length))
      connection.setRequestProperty("Content-Language", "en-US")
      connection.setUseCaches(false)
      connection.setDoOutput(true)

      val wr = new DataOutputStream(connection.getOutputStream())
      wr.write(json.getBytes())
      wr.flush()
      wr.close()

      taskExecutor.execute(new SendRunnable(connection))
  }
  
}