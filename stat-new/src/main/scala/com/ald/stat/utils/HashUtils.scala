package com.ald.stat.utils

import org.apache.hadoop.util.hash.Hash

object HashUtils {
  def getHash(key: String): Int= {
    Hash.getInstance(Hash.MURMUR_HASH).hash(key.getBytes())
  }
}
