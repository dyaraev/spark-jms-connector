package io.github.dyaraev.spark.connector.jms.example.utils

import io.github.dyaraev.spark.connector.jms.example.utils.Implicits._
import org.apache.spark.sql.types.DataType

import java.security.SecureRandom
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor
import java.time.{Instant, LocalDateTime, ZoneId}
import scala.util.Try

trait GenStringField {

  def name: String

  def sparkType: DataType

  def generate(fileNum: Int, rowNum: Int): Try[String]
}

object GenStringField {

  private lazy val DefaultFormatter = Try(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  private lazy val DefaultZoneId = Try(ZoneId.systemDefault())
  private lazy val Random = new SecureRandom()

  def oneOf(name: String, sparkType: DataType, values: IndexedSeq[String]): Try[GenStringField] =
    Try(OneOf(name, sparkType, values))

  def datetime(name: String, sparkType: DataType, min: LocalDateTime, max: LocalDateTime): Try[GenStringField] =
    Try(RandomDateTime(name, sparkType, min, max))

  def randomInt(name: String, sparkType: DataType, bound: Int): Try[GenStringField] =
    Try(RandomIntBounded(name, sparkType, bound))

  def withLogic(name: String, sparkType: DataType, logic: (Int, Int) => Try[String]): Try[GenStringField] =
    Try(new WithLogic(name, sparkType, logic))

  private case class OneOf(name: String, sparkType: DataType, values: IndexedSeq[String]) extends GenStringField {

    override def generate(fileNum: Int, rowNum: Int): Try[String] = pick(values)

    private def pick(values: IndexedSeq[String]): Try[String] = {
      for {
        idx <- Try(Random.nextInt(values.length))
        value <- Try(values(idx))
      } yield value
    }
  }

  private case class RandomDateTime(name: String, sparkType: DataType, min: LocalDateTime, max: LocalDateTime)
      extends GenStringField {

    override def generate(fileNum: Int, rowNum: Int): Try[String] = {
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

  private case class RandomIntBounded(name: String, sparkType: DataType, bound: Int) extends GenStringField {

    override def generate(fileNum: Int, rowNum: Int): Try[String] = Try(Random.nextInt(bound)).map(_.toString)
  }

  private class WithLogic(val name: String, val sparkType: DataType, logic: (Int, Int) => Try[String])
      extends GenStringField {

    override def generate(fileNum: Int, rowNum: Int): Try[String] = logic(fileNum, rowNum)
  }
}
