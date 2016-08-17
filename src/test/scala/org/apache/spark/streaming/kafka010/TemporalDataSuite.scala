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

import java.util.{HashMap => JHashMap}

import com.typesafe.config.ConfigFactory
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkFunSuite}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.JavaConverters._
import scala.util.{Random, Try}

private[kafka010] trait TemporalDataSuite extends SparkFunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll
  with Eventually
  with Logging
  with Producer {

  val kafkaTopic: String

  private lazy val config = ConfigFactory.load()
  val totalRegisters = 10000

  /**
   * Spark Properties
   */
  val conf = new SparkConf()
    .setAppName("kafka-receiver-example")
    .setIfMissing("spark.master", "local[*]")
  var sc: SparkContext = null
  var ssc: StreamingContext = null
  val preferredHosts = LocationStrategies.PreferConsistent

  /**
   * zookeeper Properties
   */
  val zkHosts = Try(config.getString("zookeeper.hosts")).getOrElse(DefaultZookeeperConnection)
  private val zkConnectionTimeout = 60000
  private val zkSessionTimeout = 6000
  val zkUtils = ZkUtils(zkHosts, zkSessionTimeout, zkConnectionTimeout, false)

  /**
   * Kafka Properties
   */
  val DefaultKafkaTopic = "topic-test"
  val kafkaHosts = Try(config.getString("kafka.hosts")).getOrElse(DefaultKafkaConnection)

  /** Create a Kafka topic and wait until it is propagated to the whole cluster */
  def createTopic(topic: String, partitions: Int): Unit = {
    AdminUtils.createTopic(zkUtils, topic, partitions, 1)
    // wait until metadata is propagated
    (0 until partitions).foreach { p =>
      Thread.sleep(1000)
    }
  }

  /** Create a Kafka topic and wait until it is propagated to the whole cluster */
  def deleteTopic(topic: String): Unit = {
    AdminUtils.deleteTopic(zkUtils, topic)
    // wait until metadata is propagated
    Thread.sleep(1000)
  }

  def getSparkKafkaParams: collection.Map[String, Object] = {
    val kp = new JHashMap[String, Object]()
    kp.put("bootstrap.servers", kafkaHosts)
    kp.put("key.deserializer", classOf[StringDeserializer])
    kp.put("value.deserializer", classOf[StringDeserializer])
    kp.put("group.id", s"test-consumer-${Random.nextInt}-${System.currentTimeMillis}")
    kp.asScala
  }

  before {

    sc = new SparkContext(conf)
    ssc = new StreamingContext(sc, Seconds(1))

    createTopic(kafkaTopic, 1)

  }

  after {

    deleteTopic(kafkaTopic)

    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
    if (sc != null) {
      sc.stop()
      sc = null
    }
    System.gc()
  }

  override def afterAll: Unit = {
    System.gc()
  }
}
