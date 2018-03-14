package com.bipf.kafka

import java.util.{ Date, Properties }

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }

import scala.util.Random

class KafkaProducerClient {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "akka-kafka-producer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  def send(message: ProducerRecord[String, String]) {
    producer.send(message)
  }

  def close() {
    producer.close()

  }

}