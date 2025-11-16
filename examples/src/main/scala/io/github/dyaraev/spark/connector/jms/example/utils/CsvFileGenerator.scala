package io.github.dyaraev.spark.connector.jms.example.utils

import cats.implicits._
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.nio.file.{Files, Path}
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.{Success, Try}

private class CsvFileGenerator(outputPath: Path, fields: List[GenStringField], runId: String, numRecords: Int) {

  import CsvFileGenerator.logger

  private val schema: StructType = StructType(fields.map(f => StructField(f.name, f.sparkType)))

  private def runWithInterval(interval: Duration, stopRef: AtomicBoolean): Future[Unit] = Future {
    var counter = 0
    val header = createCsvLine(fields.map(_.name))
    while (!stopRef.get()) {
      generateBatch(counter)
        .flatMap(records => writeToFile(header :: records, counter))
        .recover { case e => logger.error(s"Failed to generate file", e) }
      Try(Thread.sleep(interval.toMillis))
        .recover { case e => logger.error("Error while sleeping", e) }
      counter += 1
    }
  }

  private def generateBatch(fileNum: Int): Try[List[String]] = {
    (0 until numRecords).toList.traverse(generateRow(fileNum, _))
  }

  private def generateRow(fileNum: Int, rowNum: Int): Try[String] = {
    fields.map(_.generate(fileNum, rowNum)).sequence.map(createCsvLine)
  }

  private def writeToFile(lines: List[String], fileNum: Int): Try[Unit] = Try {
    for {
      path <- resolveFilePath(fileNum)
      content = lines.mkString("", "\n", "\n")
      result <- Try(Files.writeString(path, content))
      _ = logger.info(s"Generated file: $path")
    } yield result
  }

  private def resolveFilePath(counter: Int): Try[Path] = Try {
    outputPath.resolve(f"$runId-$counter%06d.csv")
  }

  private def createCsvLine(values: List[String]): String = {
    values.mkString(",")
  }
}

object CsvFileGenerator {

  private val logger = Logger(getClass)

  private val WaitTimeout = 30.seconds

  def withGenerator(outputPath: Path, interval: Duration, numRecords: Int)(
      f: StructType => Try[Unit]
  ): Try[Unit] = {
    val stopRef = new AtomicBoolean(false)
    newGenerator(outputPath, numRecords).flatMap { generator =>
      val future = generator.runWithInterval(interval, stopRef)
      val result = Try(f(generator.schema).get)

      stopRef.set(true)
      Try[Unit](Await.ready(future, WaitTimeout))
        .recover { case e: Throwable => logger.warn("Message receiver execution error", e) }

      result
    }
  }

  private def newGenerator(outputPath: Path, numRecords: Int): Try[CsvFileGenerator] = {
    val runId = UUID.randomUUID().toString
    for {
      animals <- loadStringsFromResource("animals.txt")
      colors <- loadStringsFromResource("colors.txt")
      maxDateTime <- Try(LocalDateTime.now())
      minDateTime <- Try(maxDateTime.minus(30, ChronoUnit.DAYS))
      fields <- createFields(animals, colors, minDateTime, maxDateTime, numRecords, runId)
      _ <- createOutputDirectory(outputPath)
    } yield new CsvFileGenerator(outputPath, fields, runId, numRecords)
  }

  private def createFields(
      animals: IndexedSeq[String],
      colors: IndexedSeq[String],
      minDateTime: LocalDateTime,
      maxDateTime: LocalDateTime,
      numRecords: Int,
      runId: String,
  ): Try[List[GenStringField]] = {
    List(
      GenStringField.withLogic("run_id", StringType, (_, _) => Success(runId)),
      GenStringField.withLogic("file_num", IntegerType, (fn, _) => Success(fn.toString)),
      GenStringField.withLogic("row_num", IntegerType, (_, rn) => Success(rn.toString)),
      GenStringField.oneOf("animal", StringType, animals),
      GenStringField.oneOf("color", StringType, colors),
      GenStringField.randomInt("number", IntegerType, numRecords),
      GenStringField.datetime("datetime", StringType, minDateTime, maxDateTime),
    ).sequence
  }

  private def loadStringsFromResource(resource: String): Try[IndexedSeq[String]] = Try {
    Source
      .fromResource(resource)
      .getLines()
      .map(_.trim)
      .filter(_.nonEmpty)
      .toIndexedSeq
  }

  private def createOutputDirectory(directory: Path): Try[Unit] = Try {
    if (!Files.exists(directory)) Files.createDirectories(directory)
  }
}
