package io.github.dyaraev.spark.connector.jms.common.metadata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.HDFSMetadataLog

import scala.reflect.ClassTag

abstract class SparkMetadataLog[T <: LogEntry: ClassTag](spark: SparkSession, checkpointLocation: String)
    extends HDFSMetadataLog[Array[T]](spark, checkpointLocation)
