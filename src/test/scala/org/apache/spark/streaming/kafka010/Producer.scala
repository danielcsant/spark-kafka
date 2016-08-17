/*
 * Copyright (C) 2016 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.kafka010

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

trait Producer {

  val DefaultZookeeperConnection = "127.0.0.1:2181"
  val DefaultKafkaConnection = "127.0.0.1:9092"
  val mandatoryOptions: Map[String, Any] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "acks" -> "all",
    "batch.size" -> 16384,
    "linger.ms" -> 1,
    "buffer.memory" -> 33554432,
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")

  def extractOptions(properties: Map[String, Any]): Properties = {
    val props = new Properties()
    properties.foreach { case (key, value) => props.put(key, value.toString) }
    props
  }

  def getProducer(properties: Map[String, Any]): KafkaProducer[String, String] = {
    new KafkaProducer[String, String](extractOptions(properties))
  }

  def close(kafkaProducer: KafkaProducer[String, String]): Unit = kafkaProducer.close()

  def send(kafkaProducer: KafkaProducer[String, String], topic: String, message: String): Unit = {
    val record = new ProducerRecord(topic, "", message)
    kafkaProducer.send(record)
  }
}