package org.cripac.isee.vpe.util.kafka

import java.util
import javax.annotation.{Nonnull, Nullable}

import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.NetworkException
import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}
import org.cripac.isee.vpe.common.Topic
import org.cripac.isee.vpe.util.logging.{ConsoleLogger, Logger}

import scala.collection.JavaConversions._

/**
  * Because some bugs of scala, the
  *
  * @author Ken Yu
  */
object KafkaHelper {
  /**
    * Send a message to Kafka with provided producer. Debug info is output to given logger.
    *
    * @param topic     The Kafka topic to send to.
    * @param key       Key of the message.
    * @param value     Value of the message.
    * @param producer  The Kafka producer to use to send the message.
    * @param extLogger The logger to output debug info.
    * @tparam K Type of the key.
    * @tparam V Type of the value.
    */
  def sendWithLog[K, V](@Nonnull topic: Topic,
                        @Nonnull key: K,
                        @Nonnull value: V,
                        @Nonnull producer: KafkaProducer[K, V],
                        @Nullable extLogger: Logger) {
    // Check if logger is provided. If not, create a console logger.
    val logger = if (extLogger == null) new ConsoleLogger() else extLogger
    // Send the message.
    logger debug ("Sending to Kafka <" + topic + ">\t" + key)
    val future = producer send new ProducerRecord[K, V](topic.NAME, key, value)
    // Retrieve sending report.
    try {
      val recMeta = future get;
      logger debug ("Sent to Kafka" + " <"
        + recMeta.topic + "-"
        + recMeta.partition + "-"
        + recMeta.offset + ">\t" + key)
    } catch {
      case e: InterruptedException =>
        logger error("Interrupted when retrieving Kafka sending result.", e)
    }
  }

  /**
    * Create a KakfaCluster with given Kafka parameters.
    *
    * @param kafkaParams Parameters of the Kafka cluster.
    * @return A KafkaCluster instance.
    */
  def createKafkaCluster(@Nonnull kafkaParams: util.Map[String, String]): KafkaCluster = {
    new KafkaCluster(kafkaParams toMap)
  }

  /**
    * Submit currently consumed offsets to a Kafka cluster.
    *
    * @param kafkaCluster The Kafka cluster.
    * @param offsetRanges An array of OffsetRange.
    */
  def submitOffset(@Nonnull kafkaCluster: KafkaCluster,
                   @Nonnull offsetRanges: Array[OffsetRange]): Unit = {
    // Create a map from each topic and partition to its until offset.
    val topicAndPartitionOffsetMap = collection.mutable.Map[TopicAndPartition, Long]()
    for (o <- offsetRanges) {
      val topicAndPartition = TopicAndPartition(o.topic, o.partition)
      topicAndPartitionOffsetMap += topicAndPartition -> o.untilOffset
    }
    // Submit offsets.
    kafkaCluster setConsumerOffsets(kafkaCluster kafkaParams GROUP_ID_CONFIG, topicAndPartitionOffsetMap.toMap)
  }

  /**
    * Get fromOffsets stored at a Kafka cluster.
    *
    * @param kafkaCluster The Kafka cluster.
    * @param topics       Topics the offsets belong to.
    * @return A map from each partition of each topic to the fromOffset.
    */
  def getFromOffsets(@Nonnull kafkaCluster: KafkaCluster,
                     @Nonnull topics: util.Collection[String]
                    ): util.HashMap[TopicAndPartition, java.lang.Long] = {
    // Transform topics stored in Java to Scala set.
    val scalaTopics = new util.HashSet[String](topics) toSet
    // Retrieve partition information of the topics from the Kafka cluster.
    val partitionOrErr = kafkaCluster getPartitions scalaTopics
    if (partitionOrErr.isLeft) throw new NetworkException("Cannot retrieve partitions from Kafka cluster!")
    val topicAndPartitionSet = partitionOrErr.right.get
    // Create a map to store offsets for the topics and partitions
    val consumerOffsets = new util.HashMap[TopicAndPartition, java.lang.Long]
    // Retrieve offsets.
    val offsetsOrErr = kafkaCluster getConsumerOffsets(kafkaCluster kafkaParams GROUP_ID_CONFIG, topicAndPartitionSet)
    if (offsetsOrErr isLeft)
    // No offset (new group).
      topicAndPartitionSet foreach (consumerOffsets put(_, 0L))
    else {
      val offsets = offsetsOrErr.right.get
      // Store the offsets after checking the values.
      // If an offset is smaller than 0, change it to 0.
      offsets foreach (offset => consumerOffsets put(offset._1, if (offset._2 < 0L) 0L else offset._2))
    }
    consumerOffsets
  }
}