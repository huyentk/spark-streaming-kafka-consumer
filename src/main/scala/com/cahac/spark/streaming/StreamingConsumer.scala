package com.cahac.spark.streaming

import scalaj.http.{Http, HttpOptions}
import org.json4s.DefaultFormats
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
object StreamingConsumer {
  implicit val formats: DefaultFormats.type = DefaultFormats

  case class Behavior(user: String, item: String)

  def main(args: Array[String]): Unit = {
    val streamingContext = new StreamingContext(SparkCommon.conf, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streaming_consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("behavior")

    // Create a input direct stream
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //      {"item":"e34b237d-f014-4be8-a250-47c0bdb6d906","user":1, "type":"Behavior"}
    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreach(o => {
          val result = Http("http://127.0.0.1:5984/scala").postData(o.value())
            .header("Content-Type", "application/json")
            .header("Charset", "UTF-8")
            .option(HttpOptions.readTimeout(10000)).asString
          println(result.code)
          println(result.body)
        })
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
