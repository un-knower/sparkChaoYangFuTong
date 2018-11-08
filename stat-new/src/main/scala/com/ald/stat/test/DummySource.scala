package com.ald.stat.test

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver._

class DummySource extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Dummy Source") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    //    var count = 0
    //    while(!isStopped()) {
    //      println(count)
    //      if(count == 1){
    //        store(TestApp.record)
    //      }
    //      if(count == 3){
    //        store(TestApp.record1)
    //      }
    //
    //      if(count == 5){
    //        store(TestApp.record)
    //      }
    //      count +=1
    //      Thread.sleep(10000)
    //    }
    //  }
  }
}
