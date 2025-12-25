package io.github.dyaraev.spark.connector.jms.v2

import org.apache.spark.sql.execution.streaming.LongOffset

object SparkInternals {

  type SparkLongOffset = LongOffset

  object SparkLongOffset {
    def apply(offset: Long): SparkLongOffset = LongOffset(offset)
  }
}
