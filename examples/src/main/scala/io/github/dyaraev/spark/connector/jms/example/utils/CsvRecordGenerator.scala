package io.github.dyaraev.spark.connector.jms.example.utils

import cats.implicits._

import java.util.UUID
import scala.util.Try

class CsvRecordGenerator(fields: List[CsvValueGenerator]) {

  val runId: String = UUID.randomUUID().toString

  val header: String = createCsvLine(fields.map(_.name))

  def generateBatch(numRecords: Int): Try[List[String]] = {
    (0 until numRecords).toList.traverse(generateRow)
  }

  def generateRow(rowNum: Int): Try[String] = {
    fields.map(_.generate(runId, rowNum)).sequence.map(createCsvLine)
  }

  private def createCsvLine(values: Seq[String]): String = {
    values.mkString(",")
  }
}
