package com.wiki.big.stream

import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

object ThreadPoolBuilder {
  
  def buildExecutionContext(size: Int): ExecutionContext =
   new ExecutionContext {
    val threadPool = Executors.newFixedThreadPool(size);

    def execute(runnable: Runnable) {
        threadPool.submit(runnable)
    }

    def reportFailure(t: Throwable) {}
  } 

}