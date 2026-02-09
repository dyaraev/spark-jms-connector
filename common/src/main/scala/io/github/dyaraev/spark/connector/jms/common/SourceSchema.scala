package io.github.dyaraev.spark.connector.jms.common

import io.github.dyaraev.spark.connector.jms.common.config.MessageFormat
import io.github.dyaraev.spark.connector.jms.common.config.MessageFormat.{BinaryFormat, TextFormat}
import org.apache.spark.sql.types._

object SourceSchema {

  // noinspection ScalaWeakerAccess
  val FieldId = "id"

  // noinspection ScalaWeakerAccess
  val FieldSent = "sent"

  // noinspection ScalaWeakerAccess
  val FieldReceived = "received"

  // noinspection ScalaWeakerAccess
  val FieldValue = "value"

  val BinarySchema: StructType = StructType(
    Array(
      StructField(FieldId, StringType, nullable = false),
      StructField(FieldSent, LongType, nullable = false),
      StructField(FieldReceived, LongType, nullable = false),
      StructField(FieldValue, BinaryType, nullable = false),
    )
  )

  val TextSchema: StructType = StructType(
    Array(
      StructField(FieldId, StringType, nullable = false),
      StructField(FieldSent, LongType, nullable = false),
      StructField(FieldReceived, LongType, nullable = false),
      StructField(FieldValue, StringType, nullable = false),
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
