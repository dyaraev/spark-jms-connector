package io.github.dyaraev.spark.connector.jms.example.utils

import io.github.dyaraev.spark.connector.jms.example.utils.Implicits._

import java.security.SecureRandom
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor
import java.time.{Instant, LocalDateTime, ZoneId}
import scala.io.Source
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait CsvValueGenerator {

  def name: String

  def generate(runId: String, rowNum: Int): Try[String]
}

object CsvValueGenerator {

  private lazy val DefaultFormatter = Try(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  private lazy val DefaultZoneId = Try(ZoneId.systemDefault())
  private lazy val Random = new SecureRandom()

  /**
   * Randomly pick a value from a sequence
   */
  def oneOf(name: String, values: IndexedSeq[String]): Try[CsvValueGenerator] = {
    if (values.isEmpty) {
      Failure(new RuntimeException("Values cannot be empty"))
    } else {
      Success(OneOfValues(name, values))
    }
  }

  /**
   * Randomly pick a value from a sequence of strings loaded from a resource
   */
  def oneOf(name: String, resource: String): Try[CsvValueGenerator] = {
    try {
      val values = Source
        .fromResource(resource)
        .getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .toIndexedSeq
      oneOf(name, values)
    } catch {
      case NonFatal(e) => Failure(new RuntimeException(s"Unable to load resource: $resource", e))
    }
  }

  /**
   * Generate a datetime value between min and max (min defaults to the start of the Unix epoch, and max defaults to the
   * current date/time)
   */
  def randomDateTime(
      name: String,
      min: LocalDateTime = LocalDateTime.MIN,
      max: LocalDateTime = LocalDateTime.now(),
  ): Try[CsvValueGenerator] = Try(RandomDateTime(name, min, max))

  /**
   * Generate a positive integer value between 0 and the bound (bound defaults to `Int.MaxValue`)
   */
  def randomInt(name: String, bound: Int = Int.MaxValue): Try[CsvValueGenerator] = Try(RandomInt(name, bound))

  /**
   * Use the provided logic for generating values
   */
  def withLogic(name: String, logic: (String, Int) => Try[String]): Try[CsvValueGenerator] = Try(Logic(name, logic))

  private case class OneOfValues(name: String, values: IndexedSeq[String]) extends CsvValueGenerator {

    override def generate(runId: String, rowNum: Int): Try[String] =
      Try(values(Random.nextInt(values.length)))
  }

  private case class RandomDateTime(name: String, min: LocalDateTime, max: LocalDateTime) extends CsvValueGenerator {

    override def generate(runId: String, rowNum: Int): Try[String] = {
      for {
        minMillis <- dateTimeToMillis(min)
        maxMillis <- dateTimeToMillis(max)
        _ <- (maxMillis <= minMillis).raiseWhen {
          new IllegalArgumentException(
            s"Invalid time range: max bound ($maxMillis) must be greater than min bound ($minMillis)"
          )
        }
        generatedMillis <- Try(Random.nextLong(maxMillis - minMillis))
        generatedDateTime <- dateTimeFromMillis(minMillis + generatedMillis)
        dateTimeStr <- formatDateTime(generatedDateTime)
      } yield dateTimeStr
    }

    private def dateTimeToMillis(dt: LocalDateTime): Try[Long] = {
      DefaultZoneId.flatMap(zid => Try(dt.atZone(zid).toInstant.toEpochMilli))
    }

    private def dateTimeFromMillis(millis: Long): Try[LocalDateTime] = {
      DefaultZoneId.flatMap(zid => Try(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), zid)))
    }

    private def formatDateTime(temporal: TemporalAccessor): Try[String] = {
      DefaultFormatter.flatMap(formatter => Try(formatter.format(temporal)))
    }
  }

  private case class RandomInt(name: String, bound: Int) extends CsvValueGenerator {

    override def generate(runId: String, rowNum: Int): Try[String] =
      Try(Random.nextInt(bound)).map(_.toString)
  }

  private case class Logic(name: String, logic: (String, Int) => Try[String]) extends CsvValueGenerator {

    override def generate(runId: String, rowNum: Int): Try[String] =
      Try(logic(runId, rowNum)).flatten
  }
}
