package io.github.dyaraev.spark.connector.jms.example.utils

import cats.Traverse
import cats.implicits._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.util.Try

case class FieldSpec(fieldGenerator: Try[CsvValueGenerator], sparkType: DataType)

object FieldSpec {

  def toStructType(specs: List[FieldSpec]): Try[StructType] = {
    fieldGenerators(specs).map { fgs =>
      val types = specs.map(_.sparkType)
      val names = fgs.map(_.name)
      val fields = names.zip(types).map { case (n, t) => StructField(n, t) }
      StructType(fields)
    }
  }

  def fieldGenerators(specs: List[FieldSpec]): Try[List[CsvValueGenerator]] = {
    val fields = specs.map(_.fieldGenerator)
    Traverse[List].sequence[Try, CsvValueGenerator](fields)
  }
}
