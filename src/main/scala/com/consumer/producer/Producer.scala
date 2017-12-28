package com.consumer.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object Producer extends App {

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
  properties.setProperty("key.serializer", classOf[StringSerializer].getName)
  properties.setProperty("value.serializer", classOf[StringSerializer].getName)
  properties.setProperty("acks", "all")
  properties.setProperty("retries", "3")
  properties.setProperty("linger.ms", "1")

  val producer: KafkaProducer[Nothing, String] = new KafkaProducer[Nothing, String](properties)
  val topic = "first_topic"

  for (i <- 1 to 10) {
    val record: ProducerRecord[Nothing, String] = new ProducerRecord(topic, "Producer"+i)
    producer.send(record)
  }
  producer.close()

}
