package io.github.dyaraev.spark.connector.jms.v2

import io.github.dyaraev.spark.connector.jms.common.SourceSchema
import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._
import io.github.dyaraev.spark.connector.jms.common.config.{
  CaseInsensitiveConfigMap,
  JmsConnectionConfig,
  JmsSourceConfig,
  MessageFormat,
}
import io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.Locale

class JmsSourceProvider extends SimpleTableProvider with DataSourceRegister with Logging {

  override def shortName(): String = "jms-v2"

  override def getTable(options: CaseInsensitiveStringMap): Table = new JmsSourceProvider.JmsTable(options.toConfigMap)
}

object JmsSourceProvider {

  private class JmsTable(options: CaseInsensitiveConfigMap) extends Table with SupportsRead with SupportsWrite {

    // TODO: consider adding broker as a part of the table name
    override def name(): String = {
      val queueName = options.getRequired[String](JmsConnectionConfig.OptionQueueName).toLowerCase(Locale.ROOT)
      Seq("jms", CommonUtils.replaceNonWordChars(queueName, "_")).mkString("_")
    }

    private lazy val sourceSchema = {
      options
        .getOptional[String](JmsSourceConfig.OptionMessageFormat)
        .map(MessageFormat.apply)
        .map(SourceSchema.forFormat)
        .getOrElse(StructType(Nil))
    }

    // TODO: implement toContinuousStream
    override def capabilities(): util.Set[TableCapability] = {
      util.EnumSet.of(TableCapability.MICRO_BATCH_READ, TableCapability.STREAMING_WRITE)
    }

    override def schema(): StructType = sourceSchema

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new JmsScanBuilder(options)

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new JmsWriteBuilder(info)
  }
}
