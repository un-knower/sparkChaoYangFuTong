package com.ald.stat.utils

import java.io.Serializable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RddUtils{


  def saveRDD[K <: Serializable](rdd: RDD[K], url: String): Unit = {
    rdd.saveAsObjectFile(url)
  }

//  def readRdd[K <: Serializable](url: String): RDD[K] = {
//    sparkContext.objectFile[K](url)
//  }
}

