package io.github.dyaraev.spark.connector.jms.common

import io.github.dyaraev.spark.connector.jms.common.config.MessageFormat
import io.github.dyaraev.spark.connector.jms.common.config.MessageFormat.{BinaryFormat, TextFormat}
import org.apache.spark.sql.types._

//noinspection ScalaWeakerAccess
object SourceSchema {

  val FieldId = "id"
  val FieldSent = "sent"
  val FieldReceived = "received"
  val FieldValue = "value"

  val BinarySchema: StructType = StructType(
    Array(
      StructField(FieldId, StringType),
      StructField(FieldSent, LongType),
      StructField(FieldReceived, LongType),
      StructField(FieldValue, BinaryType),
    )
  )

  val TextSchema: StructType = StructType(
    Array(
      StructField(FieldId, StringType),
      StructField(FieldSent, LongType),
      StructField(FieldReceived, LongType),
      StructField(FieldValue, StringType),
    )
  )

  def forFormat(format: MessageFormat): StructType = {
    format match {
      case TextFormat   => TextSchema
      case BinaryFormat => BinarySchema
      case another      => throw new RuntimeException(s"Unsupported message format: $another")
    }
  }
}
