package com.example

import com.example.PCProcessors.{
  makeConsumeProduceProcessor,
  makeConsumeTopicPartitionMapProcessor,
  makeLogAndCountProcessor,
  makeProduceCallback,
  retryDelayProvider
}
import com.example.serde.{ GsonDeserializer, GsonSerializer }
import Data.{ MyRecord, RecordProcessStats }
import com.example.util.{ ClientSetup, KafkaSpecHelper, SpecBase }
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import io.confluent.parallelconsumer.{
  ParallelConsumerOptions,
  ParallelStreamProcessor,
  PollContext
}
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY
import io.confluent.parallelconsumer.ParallelStreamProcessor.createEosStreamProcessor

import java.time.Instant
import org.apache.kafka.common.serialization.Serdes

import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._
import scala.util.Random

class ParallelConsumerInitSpec extends SpecBase {

  val myRecordSerializer   = new GsonSerializer[MyRecord]
  val myRecordDeserializer = new GsonDeserializer[MyRecord](classOf[MyRecord])
  val myRecordSerde: WrapperSerde[MyRecord] =
    new WrapperSerde(myRecordSerializer, myRecordDeserializer)

  val prefix: String = suiteName
  val sourceTopic    = s"${prefix}_testTopic"
  val targetTopic    = s"${prefix}_targetTopic"
  val cGroup         = s"${prefix}_cGroup"

  "ccloud" - {

    val setup: ClientSetup = ClientSetup(configPath = Some("ccloud.ps.ksilin.dedicated_ksilin"))
    // PC needs to take over the offset commits
    setup.commonProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    // you cannot just have a function and produce - you need to give PC the producer you want to use
    // To use the produce flows you must supply a Producer in the options
    // java.lang.IllegalArgumentException: To use the produce flows you must supply a Producer in the options
    val producer = new KafkaProducer[String, String](
      setup.commonProps,
      Serdes.String().serializer(),
      Serdes.String().serializer()
    )

    "must consume data from CCloud" in {

      setup.commonProps.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        cGroup + Random.alphanumeric.take(3).mkString
      )
      val stringConsumerPar: KafkaConsumer[String, String] = makeStringConsumer(setup)

      KafkaSpecHelper.createOrTruncateTopic(setup.adminClient, sourceTopic, 2, 3)

      val howManyRecords                 = 10000
      val (recordsProduced, recordsMeta) = produceRecords(sourceTopic, 3, howManyRecords, setup.commonProps)

      val options: ParallelConsumerOptions[String, String] = ParallelConsumerOptions
        .builder()
        .ordering(KEY)
        .maxConcurrency(4)
        .consumer(stringConsumerPar)
        .commitMode(PERIODIC_CONSUMER_SYNC)
        .build()

      val eosStreamProcessor: ParallelStreamProcessor[String, String] =
        createEosStreamProcessor(options)
      eosStreamProcessor.subscribe(List(sourceTopic).asJava)

      val beforeConsumeParInstant = Instant.now()
      val recordsProcessed        = new AtomicLong()
      val processor: Consumer[PollContext[String, String]] =
        makeLogAndCountProcessor(recordsProcessed)

      eosStreamProcessor.poll(processor)

      while (recordsProcessed.get() < howManyRecords) {
        Thread.sleep(1)
        println(s"processed ${recordsProcessed.get()} records")
      }
      val afterConsumeParInstant = Instant.now()
      info(
        s"processed ${recordsProcessed.get()} records in ${afterConsumeParInstant.toEpochMilli - beforeConsumeParInstant.toEpochMilli} ms"
      )
    }

    "must consume data from CCloud with topicPartitionMap" in {

      setup.commonProps.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        cGroup + Random.alphanumeric.take(3).mkString
      )
      val stringConsumerPar: KafkaConsumer[String, String] = makeStringConsumer(setup)

      KafkaSpecHelper.createOrTruncateTopic(setup.adminClient, sourceTopic, 2, 3)

      val howManyRecords                 = 10000
      val (recordsProduced, recordsMeta) = produceRecords(sourceTopic, 3, howManyRecords, setup.commonProps)

      val options: ParallelConsumerOptions[String, String] = ParallelConsumerOptions
        .builder()
        .ordering(KEY)
        .maxConcurrency(4)
        .consumer(stringConsumerPar)
        .commitMode(PERIODIC_CONSUMER_SYNC)
        .build()

      val eosStreamProcessor: ParallelStreamProcessor[String, String] =
        createEosStreamProcessor(options)
      eosStreamProcessor.subscribe(List(sourceTopic).asJava)

      val beforeConsumeParInstant   = Instant.now()
      val stats: RecordProcessStats = makeStats(recordsProduced)
      val processor: Consumer[PollContext[String, String]] =
        makeConsumeTopicPartitionMapProcessor(stats)

      eosStreamProcessor.poll(processor)

      while (stats.totalRecordsProcessed < howManyRecords) {
        Thread.sleep(1000)
        warn(stats.statString())
      }
      val afterConsumeParInstant = Instant.now()
      info(
        s"processed ${stats.totalRecordsProcessed} records in ${afterConsumeParInstant.toEpochMilli - beforeConsumeParInstant.toEpochMilli} ms"
      )
    }

    "must consume data from and produce data to CCloud" in {

      setup.commonProps.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        cGroup + Random.alphanumeric.take(3).mkString
      )
      val stringConsumerPar: KafkaConsumer[String, String] = makeStringConsumer(setup)

      KafkaSpecHelper.createOrTruncateTopic(setup.adminClient, sourceTopic, 2, 3)
      KafkaSpecHelper.createOrTruncateTopic(setup.adminClient, targetTopic, 2, 3)

      val howManyRecords                 = 10000
      val (recordsProduced, recordsMeta) = produceRecords(sourceTopic, 20, howManyRecords, setup.commonProps)

      val options: ParallelConsumerOptions[String, String] = ParallelConsumerOptions
        .builder()
        .ordering(KEY)
        .maxConcurrency(4)
        .consumer(stringConsumerPar)
        .producer(producer)
        //  trying to induce exception on produce
        // .sendTimeout(Duration.apply(10, TimeUnit.MILLISECONDS).toJava)
        .retryDelayProvider(retryDelayProvider)
        .commitMode(PERIODIC_CONSUMER_SYNC)
        .build()

      val eosStreamProcessor: ParallelStreamProcessor[String, String] =
        createEosStreamProcessor(options)
      eosStreamProcessor.subscribe(List(sourceTopic).asJava)

      val beforeConsumeParInstant   = Instant.now()
      val recordsProducedCount      = new AtomicLong()
      val stats: RecordProcessStats = makeStats(recordsProduced)

      eosStreamProcessor.pollAndProduce(
        makeConsumeProduceProcessor(targetTopic, stats),
        makeProduceCallback(recordsProducedCount)
      )

      while (stats.totalRecordsProcessed < howManyRecords) {
        Thread.sleep(1000)
        warn(stats.statString())
      }
      val afterConsumeParInstant = Instant.now()
      info(
        s"processed ${stats.totalRecordsProcessed} records in ${afterConsumeParInstant.toEpochMilli - beforeConsumeParInstant.toEpochMilli} ms"
      )
    }
  }


}
