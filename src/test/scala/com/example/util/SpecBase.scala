package com.example.util

import com.example.Data.{MyRecord, RecordProcessStats}
import com.example.serde.JsonStringProducerCirce
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{ProducerConfig, RecordMetadata}
import org.apache.kafka.common.serialization.{Deserializer, Serdes}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.must.Matchers
import wvlet.log.LogSupport

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import nequi.circe.kafka._
import io.circe.generic.auto._

import java.util.Properties
import scala.util.Random

class SpecBase extends AnyFreeSpecLike with LogSupport with Matchers {

  def makeTypedConsumer[V](
      setup: ClientSetup,
      deserializer: Deserializer[V]
  ): KafkaConsumer[String, V] =
    new KafkaConsumer[String, V](
      setup.commonProps,
      Serdes.String().deserializer(),
      deserializer
    )

  def makeStringConsumer(setup: ClientSetup): KafkaConsumer[String, String] =
    new KafkaConsumer[String, String](
      setup.commonProps,
      Serdes.String().deserializer(),
      Serdes.String().deserializer(),
    )

  def makeStats(recordsProduced: Map[String, List[(String, MyRecord)]]): RecordProcessStats = {
    val recordsProcessedByKey: Map[String, AtomicLong] =
      recordsProduced.view.mapValues(_ => new AtomicLong(0)).toMap
    val recordsTotalByKey: Map[String, Long] =
      recordsProduced.view.mapValues(v => v.length.longValue).toMap
    val stats =
      RecordProcessStats("consumeProduceProcessor", recordsProcessedByKey, Some(recordsTotalByKey))
    stats
  }

  def produceRecords(topic: String, howManyKeys: Int, howManyRecords: Int, producerProps: Properties)(
      implicit ec: ExecutionContext
  ): (Map[String, List[(String, MyRecord)]], List[RecordMetadata]) = {
    val keys: IndexedSeq[String] =
      (1 to howManyKeys) map (_ => Random.alphanumeric.take(5).mkString)
    println(s"keys: ${keys.mkString("|")}")
    val records: List[(String, MyRecord)] = makeRecordTuples(keys, howManyRecords)

    // evaluate key cardinality
    val byKey: Map[String, List[(String, MyRecord)]] = records.groupBy(_._1)
    byKey foreach {
      case (k, values) =>
        info(s"key $k : ${values.length}")
    }

    val propCopy = new Properties()
    propCopy.putAll(producerProps)
    // short producer timeout to catch errors faster
    propCopy.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "500")
    propCopy.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "1000")
    val producer = new JsonStringProducerCirce[String, MyRecord](propCopy, topic)
    val produce: Future[List[RecordMetadata]] = Future.sequence {
      records.map {
        case (k, v) =>
          producer.produceAsync(k, v)
      }
    }
    (byKey, Await.result(produce, 30.seconds))
  }

  def makeRecordTuples(
      keys: Seq[String],
      howMany: Int,
      intervalBetweenRecordsMs: Int = 200,
      startTime: Instant = Instant.now()
  ): List[(String, MyRecord)] = {
    val tuples = (1 to howMany) map { i =>
      val key         = Random.shuffle(keys).head
      val description = Random.alphanumeric.take(1).mkString
      val timestamp   = startTime.plus(i * intervalBetweenRecordsMs, ChronoUnit.MILLIS).toEpochMilli
      val value       = MyRecord(key, description, timestamp)
      (key, value)
    }
    tuples.toList
  }

}
