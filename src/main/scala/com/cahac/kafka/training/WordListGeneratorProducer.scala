package com.cahac.kafka.training

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

object WordListGeneratorProducer {
  def main(args: Array[String]): Unit = {
    //Get the Kafka broker node
    val brokers = util.Try(args(0)).getOrElse("localhost:9092")
    //Get topic
    val topic = util.Try(args(1)).getOrElse("welcome-message")
    //Get number of event to be fired
    val events = util.Try(args(2)).getOrElse("100").toInt
    //Get interval time between each event
    val intervalEvent = util.Try(args(3)).getOrElse("1").toInt //in second
    //Get the file path of Word List
    val filePath = util.Try(args(4)).getOrElse("D:\\word-list.txt")

    val clientId = "kafka-custom-producer"
    //Create some properties
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", clientId)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val wordList = Source.fromFile(filePath).getLines().toSeq
    println("======================BEGIN=============================")

    val beforeTime = System.currentTimeMillis()
    for(i <- Range(0, events)){
      val rndWords = getRandomWordList(wordList)
      val key = i.toString()
      val value = rndWords.mkString(",")
      val data = new ProducerRecord[String, String](topic, key, value)

      println(data)
      producer.send(data)

      if(intervalEvent > 0)
        Thread.sleep(intervalEvent * 1000)
    }
    val afterTime = System.currentTimeMillis()

    println("======================END=============================")
    println("Total time: " + ((afterTime - beforeTime) / 1000) + " sec")
    producer.close()
  }

  def getRandomWordList(words: Seq[String]): Seq[String] ={

    var rdnWords = new ListBuffer[String]()
    val rnd = new Random()

    for(i <- 0 until 10){
      rdnWords += words(rnd.nextInt(words.length))
    }

    rdnWords
  }
}
