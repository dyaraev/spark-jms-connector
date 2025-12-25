package io.github.dyaraev.spark.connector.jms.v2

import org.apache.spark.sql.execution.streaming.runtime.LongOffset

object SparkInternals {

  type SparkLongOffset = LongOffset

  object SparkLongOffset {
    def apply(offset: Long): SparkLongOffset = LongOffset(offset)
  }
}
