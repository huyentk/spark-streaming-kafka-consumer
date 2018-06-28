package com.cahac.spark.streaming

import com.ibm.couchdb._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.slf4j.LoggerFactory
import scalaz._
import scalaz.concurrent.Task

object StreamingConsumer {

  case class Behavior(user: String, item: String)

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(Database.getClass)
    val typeMapping = TypeMapping(classOf[Behavior] -> "user_behavior")

//    val alice = Person("Alice", 25)

    val couch = CouchDb("127.0.0.1", 5984, https = false, "huyentk", "Huyen1312")
    val dbName = "advertisement"
    val db = couch.db(dbName, typeMapping)

    val streamingContext = new StreamingContext(SparkCommon.conf, Seconds(30))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streaming_consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("welcome-message")

    // Create a input direct stream
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    /*----Code for process received data here---*/
    stream.foreachRDD(
      rdd => {
        if(!rdd.isEmpty()){
          rdd.foreach(o => {
            val value =
            println(value)
            println(o)
//            typeMapping.get(classOf[Behavior]).foreach { mType =>
//              val actions: Task[Behavior] = for {
//                // Insert documents into the database
//                _ <- db.docs.create(Behavior())
//                //            // Retrieve all documents from the database and unserialize to Person
//                docs <- db.docs.getMany.includeDocs[Person].byTypeUsingTemporaryView(mType).build.query
//              } yield docs.getDocsData
//
//              // Execute the actions and process the result
//              actions.unsafePerformSyncAttempt match {
//                // In case of an error (left side of Either), print it
//                case -\/(e) => logger.error(e.getMessage, e)
//                // In case of a success (right side of Either), print each object
//                case \/-(a) => a.foreach(x => logger.info(x.toString))
//              }
//            }
          })
        }
      }
    )
    streamingContext.start()
    streamingContext.awaitTermination()
    couch.client.client.shutdownNow()
  }
}
