package io.github.dyaraev.spark.connector.jms.example.utils

import com.typesafe.scalalogging.Logger

import java.nio.file.{FileAlreadyExistsException, Files, Path, StandardOpenOption}
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object CsvFileGenerator {

  private val logger = Logger(getClass)

  private val ShutdownTimeoutSec = 10L

  def withGenerator(fields: List[CsvValueGenerator], outputPath: Path, interval: Duration, numRecords: Int)(
      f: () => Try[Unit]
  ): Try[Unit] = {
    createOutputDirectory(outputPath).flatMap { _ =>
      val recordGenerator = new CsvRecordGenerator(fields)
      val fileGenerator = new CsvFileGenerator(recordGenerator, numRecords, outputPath)
      val result = fileGenerator.start(interval).flatMap(_ => Try(f()).flatten)
      fileGenerator.stop().recover { case NonFatal(e) => logger.warn("Error while stopping file generator", e) }
      result
    }
  }

  private def createOutputDirectory(path: Path): Try[Unit] = {
    if (!Files.exists(path)) {
      Try(Files.createDirectories(path))
    } else if (!Files.isDirectory(path)) {
      Failure(new RuntimeException("Output path already exists and it is not a directory"))
    } else {
      Success(())
    }
  }
}

private class CsvFileGenerator(recordGenerator: CsvRecordGenerator, numRecords: Int, outputPath: Path) {

  import CsvFileGenerator.logger

  private var counter = 0

  private val executor = Executors.newSingleThreadScheduledExecutor()

  private def start(interval: Duration): Try[Unit] = Try {
    logger.info("Starting file generator ...")
    executor.scheduleAtFixedRate(() => write(), 0, interval.toMillis, TimeUnit.MILLISECONDS)
    logger.info("File generator started")
  }

  private def stop(): Try[Unit] = Try {
    logger.info("Stopping file generator ...")
    executor.shutdown()
    if (!executor.awaitTermination(CsvFileGenerator.ShutdownTimeoutSec, TimeUnit.SECONDS)) {
      executor.shutdownNow()
    }
    logger.info("File generator stopped")
  }

  private def write(): Unit = {
    resolveFilePath(counter).flatMap { path =>
      recordGenerator.generateBatch(numRecords).flatMap { lines =>
        val content = (recordGenerator.header :: lines).mkString("", "\n", "\n")
        writeToFile(content, path).map { _ =>
          logger.info(s"Generated file: $path")
          counter += 1
        }
      } recover {
        case _: FileAlreadyExistsException =>
          logger.error(s"Output file already exists, stopping generator: $path")
          executor.shutdownNow()
        case NonFatal(e) =>
          logger.error(s"Failed to generate file: $path", e)
      }
    } recover { case NonFatal(e) => logger.error("Failed to generate file", e) }
  }

  private def resolveFilePath(counter: Int): Try[Path] = Try {
    outputPath.resolve(f"${recordGenerator.runId}-$counter%06d.csv")
  }

  private def writeToFile(content: String, path: Path): Try[Unit] = Try {
    Files.writeString(path, content, StandardOpenOption.CREATE_NEW)
  }
}
