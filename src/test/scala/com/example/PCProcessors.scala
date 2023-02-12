package com.example

import Data.RecordProcessStats
import io.confluent.parallelconsumer.ParallelStreamProcessor.ConsumeProduceResult
import io.confluent.parallelconsumer.{ PollContext, RecordContext, RecordContextInternal }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import wvlet.log.LogSupport

import java.util.concurrent.atomic.AtomicLong
import java.util.function
import java.util.function.Consumer
import scala.collection.mutable
import scala.util.Random
import scala.jdk.StreamConverters._
import scala.jdk.CollectionConverters._

case object PCProcessors extends LogSupport {

  val retryDelayProvider
      : java.util.function.Function[RecordContext[String, String], java.time.Duration] = {
    recordContext: RecordContext[String, String] =>
      info(s"reevaluating delay: ${recordContext.getNumberOfFailedAttempts}")
      val numberOfFailedAttempts = recordContext.getNumberOfFailedAttempts
      val delayMillis =
        100 // (long)(baseDelaySecond * Math.pow(multiplier, numberOfFailedAttempts) * 1000);
      java.time.Duration.ofMillis(delayMillis)
  }

  def makeConsumeProduceProcessor(
      targetTopic: String,
      stats: RecordProcessStats
  ): function.Function[PollContext[String, String], ProducerRecord[String, String]] = {
    (context: PollContext[String, String]) =>
      val threadId: Long = Thread.currentThread().getId
      val inputRecord    = context.getSingleConsumerRecord
      info(
        s"processing source record: ${inputRecord.key()}|${inputRecord.value()}|${inputRecord.partition()}"
      )
      val haveWeSeenThisRecordAlready = context.getSingleRecord.getNumberOfFailedAttempts > 0
      if (haveWeSeenThisRecordAlready)
        info(
          s"hello again, reprocessing record after ${context.getSingleRecord.getNumberOfFailedAttempts} failures"
        )
      else {
        stats.increment(inputRecord.key())
        stats.incrementThread(threadId.toString)
      }
      val record = new ProducerRecord[String, String](
        targetTopic,
        inputRecord.key(),
        inputRecord.value() + "processed"
      )
      info(
        s"producing target record: ${record.key()}|${record.value()}|${record.partition()} on thread $threadId"
      )
      record
  }

  def makeConsumeTopicPartitionMapProcessor(
      stats: RecordProcessStats
  ): Consumer[PollContext[String, String]] = { (context: PollContext[String, String]) =>
    val record = context.getSingleConsumerRecord

    val byTopicPartition: Map[TopicPartition, mutable.Set[RecordContext[String, String]]] =
      context.getByTopicPartitionMap.asScala.view.mapValues(_.asScala).toMap

    byTopicPartition foreach {
      case (tp, records) =>
        info(s"processing tp : $tp")
        records foreach { rc =>
          info(
            s"processing record in tp: ${rc.key()}|${rc.value()}|${rc.partition()}|${rc.getNumberOfFailedAttempts}"
          )
        }
    }

    val internals: List[RecordContextInternal[String, String]] =
      context.streamInternal().toScala(List)
    internals.map { rci =>
      rci.getWorkContainer.getWorkType
      rci.getWorkContainer.isInFlight
    }

    info(s"processing record: ${record.key()}|${record.value()}|${record.partition()}")
    stats.increment(context.key())
  }

  def makeLogAndCountProcessor(
      recordsProcessed: AtomicLong
  ): Consumer[PollContext[String, String]] = { (context: PollContext[String, String]) =>
    val record = context.getSingleConsumerRecord
    info(s"processing record: ${record.key()}|${record.value()}|${record.partition()}")
    recordsProcessed.getAndIncrement()
  }

  def makeProduceCallback(
      recordsProcessed: AtomicLong
  ): Consumer[ConsumeProduceResult[String, String, String, String]] = {
    (res: ConsumeProduceResult[String, String, String, String]) =>
      info(s"poll context: ${res.getIn}")
      info(s"ProducerRecord: ${res.getOut}")
      info(s"meta: ${res.getMeta.partition()} : ${res.getMeta.offset()}")
      recordsProcessed.getAndIncrement()
  }

  def makeExternalSystemSimulationProcessor(
      recordsProcessed: AtomicLong,
      maxTimeoutMs: Int = 100,
      constantTimeout: Boolean = false,
      failureProbabilityPercentage: Int = 0
  ): Consumer[PollContext[String, String]] = {
    val failureProbabilityThreshold = Math.min(Math.max(0, failureProbabilityPercentage), 100)
    (context: PollContext[String, String]) => {
      val record = context.getSingleConsumerRecord
      info(s"processing record: ${record.key()}|${record.value()}|${record.partition()}")
      if (Random.nextInt(100) < failureProbabilityThreshold) {
        throw new IllegalStateException(
          s"external system call failed for record ${record.key()}|${record.value()}|${record.partition()}"
        )
      }
      val timeout = if (constantTimeout) maxTimeoutMs else Random.nextInt(maxTimeoutMs)
      Thread.sleep(timeout)
      recordsProcessed.getAndIncrement()
    }
  }
}
