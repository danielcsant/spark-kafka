[![Coverage Status](https://coveralls.io/repos/github/Stratio/spark-kafka/badge.svg?branch=master)](https://coveralls.io/github/Stratio/spark-kafka?branch=master)

# Kafka-Receiver

Kafka-Receiver is a library that allows the user to read data with Spark Streaming from Kafka 0.10.0 version


## Requirements

This library requires Spark 1.6.2, Scala 2.11 and Kafka 0.10.0.0

## Using the library

There are two ways of using Kafka-Receiver library:

The first one is to add the next dependency in your pom.xml:

  ```
  <dependency>
    <groupId>com.stratio.receiver</groupId>
    <artifactId>spark-kafka</artifactId>
    <version>LATEST</version>
  </dependency>
  ```

The other one is to clone the full repository and build the project:

  ```
  git clone https://github.com/Stratio/spark-kafka.git
  mvn clean install
  ```

### Build

- To package it:

    `mvn clean package`


### Direct Approach (No Receivers)

This new receiver-less "direct" approach has been introduced in Spark 1.3 to ensure stronger end-to-end guarantees. Instead of using receivers to receive data, this approach periodically queries Kafka for the latest offsets in each topic+partition, and accordingly defines the offset ranges to process in each batch. When the jobs to process the data are launched, Kafka's simple consumer API is used to read the defined ranges of offsets from Kafka (similar to read files from a file system). Note that this is an experimental feature introduced in Spark 1.3 for the Scala and Java API, in Spark 1.4 for the Python API.

This approach has the following advantages over the receiver-based approach (i.e. Approach 1).

- *Simplified Parallelism:* No need to create multiple input Kafka streams and union them. With `directStream`, Spark Streaming will create as many RDD partitions as there are Kafka partitions to consume, which will all read data from Kafka in parallel. So there is a one-to-one mapping between Kafka and RDD partitions, which is easier to understand and tune.

- *Efficiency:* Achieving zero-data loss in the first approach required the data to be stored in a Write Ahead Log, which further replicated the data. This is actually inefficient as the data effectively gets replicated twice - once by Kafka, and a second time by the Write Ahead Log. This second approach eliminates the problem as there is no receiver, and hence no need for Write Ahead Logs. As long as you have sufficient Kafka retention, messages can be recovered from Kafka.

- *Exactly-once semantics:* The first approach uses Kafka's high level API to store consumed offsets in Zookeeper. This is traditionally the way to consume data from Kafka. While this approach (in combination with write ahead logs) can ensure zero data loss (i.e. at-least once semantics), there is a small chance some records may get consumed twice under some failures. This occurs because of inconsistencies between data reliably received by Spark Streaming and offsets tracked by Zookeeper. Hence, in this second approach, we use simple Kafka API that does not use Zookeeper. Offsets are tracked by Spark Streaming within its checkpoints. This eliminates inconsistencies between Spark Streaming and Zookeeper/Kafka, and so each record is received by Spark Streaming effectively exactly once despite failures. In order to achieve exactly-once semantics for output of your results, your output operation that saves the data to an external data store must be either idempotent, or an atomic transaction that saves results and offsets (see [Semantics of output operations](streaming-programming-guide.html#semantics-of-output-operations) in the main programming guide for further information).

Note that one disadvantage of this approach is that it does not update offsets in Zookeeper, hence Zookeeper-based Kafka monitoring tools will not show progress. However, you can access the offsets processed by this approach in each batch and update Zookeeper yourself (see below).

Next, we discuss how to use this approach in your streaming application.

1. **Linking:** This approach is supported only in Scala/Java application. Link your SBT/Maven project with the following artifact (see [Linking section](streaming-programming-guide.html#linking) in the main programming guide for further information).

		groupId = org.apache.spark
		artifactId = spark-streaming-kafka-0-8_{{site.SCALA_BINARY_VERSION}}
		version = {{site.SPARK_VERSION_SHORT}}

2. **Programming:** In the streaming application code, import `KafkaUtils` and create an input DStream as follows.

	<div class="codetabs">
	<div data-lang="scala" markdown="1">
		import org.apache.spark.streaming.kafka._

		val directKafkaStream = KafkaUtils.createDirectStream[
			[key class], [value class], [key decoder class], [value decoder class] ](
			streamingContext, [map of Kafka parameters], [set of topics to consume])

	You can also pass a `messageHandler` to `createDirectStream` to access `MessageAndMetadata` that contains metadata about the current message and transform it to any desired type.
	See the [API docs](api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$)
	and the [example]({{site.SPARK_GITHUB_URL}}/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/DirectKafkaWordCount.scala).
	</div>
	<div data-lang="java" markdown="1">
		import org.apache.spark.streaming.kafka.*;

		JavaPairInputDStream<String, String> directKafkaStream =
			KafkaUtils.createDirectStream(streamingContext,
				[key class], [value class], [key decoder class], [value decoder class],
				[map of Kafka parameters], [set of topics to consume]);

	You can also pass a `messageHandler` to `createDirectStream` to access `MessageAndMetadata` that contains metadata about the current message and transform it to any desired type.
	See the [API docs](api/java/index.html?org/apache/spark/streaming/kafka/KafkaUtils.html)
	and the [example]({{site.SPARK_GITHUB_URL}}/blob/master/examples/src/main/java/org/apache/spark/examples/streaming/JavaDirectKafkaWordCount.java).

	</div>
	<div data-lang="python" markdown="1">
		from pyspark.streaming.kafka import KafkaUtils
		directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

	You can also pass a `messageHandler` to `createDirectStream` to access `KafkaMessageAndMetadata` that contains metadata about the current message and transform it to any desired type.
	By default, the Python API will decode Kafka data as UTF8 encoded strings. You can specify your custom decoding function to decode the byte arrays in Kafka records to any arbitrary data type. See the [API docs](api/python/pyspark.streaming.html#pyspark.streaming.kafka.KafkaUtils)
	and the [example]({{site.SPARK_GITHUB_URL}}/blob/master/examples/src/main/python/streaming/direct_kafka_wordcount.py).
	</div>
	</div>

	In the Kafka parameters, you must specify either `metadata.broker.list` or `bootstrap.servers`.
	By default, it will start consuming from the latest offset of each Kafka partition. If you set configuration `auto.offset.reset` in Kafka parameters to `smallest`, then it will start consuming from the smallest offset. 

	You can also start consuming from any arbitrary offset using other variations of `KafkaUtils.createDirectStream`. Furthermore, if you want to access the Kafka offsets consumed in each batch, you can do the following. 

	<div class="codetabs">
	<div data-lang="scala" markdown="1">
		// Hold a reference to the current offset ranges, so it can be used downstream
		var offsetRanges = Array[OffsetRange]()
		
		directKafkaStream.transform { rdd =>
		  offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
		  rdd
		}.map {
                  ...
		}.foreachRDD { rdd =>
		  for (o <- offsetRanges) {
		    println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
		  }
		  ...
		}
	</div>
	<div data-lang="java" markdown="1">
		// Hold a reference to the current offset ranges, so it can be used downstream
		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
		
		directKafkaStream.transformToPair(
		  new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
		    @Override
		    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
		      OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
		      offsetRanges.set(offsets);
		      return rdd;
		    }
		  }
		).map(
		  ...
		).foreachRDD(
		  new Function<JavaPairRDD<String, String>, Void>() {
		    @Override
		    public Void call(JavaPairRDD<String, String> rdd) throws IOException {
		      for (OffsetRange o : offsetRanges.get()) {
		        System.out.println(
		          o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
		        );
		      }
		      ...
		      return null;
		    }
		  }
		);
	</div>
	<div data-lang="python" markdown="1">
		offsetRanges = []

		def storeOffsetRanges(rdd):
		    global offsetRanges
		    offsetRanges = rdd.offsetRanges()
		    return rdd

		def printOffsetRanges(rdd):
		    for o in offsetRanges:
		        print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

		directKafkaStream\
		    .transform(storeOffsetRanges)\
		    .foreachRDD(printOffsetRanges)
	</div>
   	</div>

	You can use this to update Zookeeper yourself if you want Zookeeper-based Kafka monitoring tools to show progress of the streaming application.

	Note that the typecast to HasOffsetRanges will only succeed if it is done in the first method called on the directKafkaStream, not later down a chain of methods. You can use transform() instead of foreachRDD() as your first method call in order to access offsets, then call further Spark methods. However, be aware that the one-to-one mapping between RDD partition and Kafka partition does not remain after any methods that shuffle or repartition, e.g. reduceByKey() or window().


# License #

Licensed to STRATIO (C) under one or more contributor license agreements.
See the NOTICE file distributed with this work for additional information
regarding copyright ownership.  The STRATIO (C) licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
