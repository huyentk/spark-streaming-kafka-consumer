package com.cahac.spark.streaming

import org.apache.spark.SparkConf

object SparkCommon {
  lazy val conf = new SparkConf()
  conf.setAppName("streaming-kafka-consumer")
    .setMaster("local[2]")
}
