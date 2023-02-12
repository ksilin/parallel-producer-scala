package com.example

import java.lang
import java.util.concurrent.atomic.AtomicLong
import scala.collection.{ immutable, mutable }

case object Data {

  case class MyRecord(name: String, description: String, timestamp: lang.Long)

  case class RecordProcessStats(
      name: String,
      recordsProcessedByKey: Map[String, AtomicLong],
      maybeTotalRecordsByKey: Option[Map[String, Long]] = None
  ) {

    val recordsProcessedByThread: mutable.Map[String, AtomicLong] =
      mutable.Map.empty[String, AtomicLong]
    def incrementThread(threadKey: String): Long =
      recordsProcessedByThread.getOrElseUpdate(threadKey, new AtomicLong()).getAndIncrement()

    def totalRecordsProcessed: Long = recordsProcessedByKey.values.map(_.get()).sum

    def increment(key: String): Option[Long] =
      recordsProcessedByKey.get(key).map(_.getAndIncrement())

    // def recordProcessedSinceLastIterations

    def statString(): String = {
      val l = List(s"processed ${totalRecordsProcessed} records")

      val perKeyStrings: Iterable[String] = recordsProcessedByKey map {
        case (k, v) => s"$k : ${v.get()}"
      }
      val maybeHistogramString: Option[Iterable[String]] =
        maybeTotalRecordsByKey map (makeHistogramString(_)(recordsProcessedByKey))
      val perThreadStrings: mutable.Iterable[String] = recordsProcessedByThread map {
        case (k, v) => s"thread $k : ${v.get()}"
      }
      val withoutHistoStrings = l.appendedAll(perKeyStrings).appendedAll(perThreadStrings)
      val finalStrings =
        maybeHistogramString.fold(withoutHistoStrings)(withoutHistoStrings.appendedAll)
      finalStrings.mkString("\n")
    }
  }

  private def makeHistogramString(
      totals: Map[String, Long]
  ): Map[String, AtomicLong] => Iterable[String] = { byKey =>
    val combinedWithTotals: Map[String, (AtomicLong, Long)] = byKey.map {
      case (k, v) => k -> (v, totals.getOrElse(k, 0))
    }
    val mappedToPercentage: immutable.Iterable[String] = combinedWithTotals map {
      case (k: String, (current: AtomicLong, total: Long)) =>
        val percents: Int = ((current.get() / total.floatValue) * 100).toInt
        s"$k : ${StatColumns.col(percents)}"
    }
    mappedToPercentage
  }

  case object StatColumns {
    val col: Seq[String] = (0 to 100) map { i: Int =>
      (List.fill(i)('█') ++ List.fill(100 - i)('.').appendedAll(List('|'))).mkString
    }
  }
}
