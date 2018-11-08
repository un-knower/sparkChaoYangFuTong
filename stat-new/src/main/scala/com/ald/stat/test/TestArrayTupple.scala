package com.ald.stat.test

import scala.collection.mutable.ArrayBuffer

object TestArrayTupple {

  def main(args: Array[String]): Unit = {
    val arr = ArrayBuffer[(String, Long)]()
    arr.insert(0, ("1", 1))
    arr.+=(("2", 1));
    println(arr)
  }
}
