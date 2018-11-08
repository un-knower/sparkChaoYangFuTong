package com.ald.stat.utils

import com.alibaba.fastjson.JSON

import scala.reflect.ClassTag

trait Copyable {
  def copy[S, D](s: S)(implicit m: ClassTag[D]): D = {
    JSON.parseObject(JSON.toJSONString(s, true), m.runtimeClass)
  }
}
