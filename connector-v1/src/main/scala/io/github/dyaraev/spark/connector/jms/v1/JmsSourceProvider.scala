package io.github.dyaraev.spark.connector.jms.v1

import io.github.dyaraev.spark.connector.jms.common.SourceSchema
import io.github.dyaraev.spark.connector.jms.common.config.MessageFormat.{BinaryFormat, TextFormat}
import io.github.dyaraev.spark.connector.jms.common.config.{CaseInsensitiveConfigMap, JmsSinkConfig, JmsSourceConfig}
import io.github.dyaraev.spark.connector.jms.common.metadata.LogEntry.Implicits._
import io.github.dyaraev.spark.connector.jms.common.metadata.LogEntry.{BinaryLogEntry, TextLogEntry}
import io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

import java.util.Locale

class JmsSourceProvider extends StreamSourceProvider with StreamSinkProvider with DataSourceRegister with Logging {

  override def shortName(): String = JmsSourceProvider.FormatName

  // TODO: consider adding broker as a part of the table name
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String],
  ): (String, StructType) = {
    assert(schema.isEmpty, "JMS source doesn't support custom schemas")
    val options = CaseInsensitiveConfigMap(parameters)
    val config = JmsSourceConfig.fromOptions(options)
    val queueName = config.connection.queue.toLowerCase(Locale.ROOT)
    val sourceName = Seq("jms", CommonUtils.replaceNonWordChars(queueName, "_")).mkString("_")
    (sourceName, SourceSchema.forFormat(config.messageFormat))
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String],
  ): Source = {
    assert(schema.isEmpty, "JMS source doesn't support custom schemas")
    val options = CaseInsensitiveConfigMap(parameters)
    val config = JmsSourceConfig.fromOptions(options)
    config.messageFormat match {
      case TextFormat   => new JmsSource[TextLogEntry](SourceSchema.TextSchema, sqlContext, config, metadataPath)
      case BinaryFormat => new JmsSource[BinaryLogEntry](SourceSchema.BinarySchema, sqlContext, config, metadataPath)
      case format       => throw new RuntimeException(s"Unsupported message format '$format'")
    }
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode,
  ): Sink = {
    assert(outputMode == OutputMode.Append(), s"JMS sink doesn't support output mode '$outputMode'")
    assert(partitionColumns.isEmpty, "JMS sink doesn't support partitioning")
    val options = CaseInsensitiveConfigMap(parameters)
    val config = JmsSinkConfig.fromOptions(options)
    new JmsSink(config)
  }
}

object JmsSourceProvider {

  // noinspection ScalaWeakerAccess
  val FormatName: String = "jms-v1"
}
