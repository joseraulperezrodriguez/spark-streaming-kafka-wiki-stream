package com.wiki.stream.net

import java.net.HttpURLConnection
import java.io.BufferedReader
import java.io.InputStreamReader

class SendRunnable(connection: HttpURLConnection) extends Runnable {
  
  def run() {
    val is = connection.getInputStream()
    val rd = new BufferedReader(new InputStreamReader(is))
    rd.lines()
    rd.close()
  }
  
}