package org.apache.spark.sql.jms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.execution.streaming.{LongOffset, SerializedOffset}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

object SparkInternals {

  type SparkLongOffset = LongOffset
  type SparkSerializedOffset = SerializedOffset

  object SparkLongOffset {
    def apply(offset: Long): SparkLongOffset = LongOffset(offset)
  }

  object SparkSerializedOffset {
    def apply(json: String): SparkSerializedOffset = SerializedOffset(json)
  }

  def createDataFrame(
      sqlContext: SQLContext,
      rdd: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean,
  ): DataFrame = {
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming)
  }

  def sameTypes(dt1: DataType, dt2: DataType): Boolean = dt1.sameType(dt2)
}
