package com.consumer.producer

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

object Consumer extends  App {

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
  properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
  properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)
  properties.setProperty("group.id", "test")
  properties.setProperty("enable.auto.commit", "true")
  properties.setProperty("auto.commit.interval.ms", "1000")
  properties.setProperty("auto.offset.reset", "earliest")

  val topic = "first_topic"

  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)
  consumer.subscribe(Collections.singletonList(topic))

  while(true) {
    val records : ConsumerRecords[String, String] = consumer.poll(1000)

    records.forEach{r =>
      println(r.partition())
      println(r.offset())
      println(r.key())
      println(r.value())
    }

  }

}
