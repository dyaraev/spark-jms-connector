package io.github.dyaraev.spark.connector.jms

import io.github.dyaraev.spark.connector.jms.utils.Utils
import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class JmsTable(config: JmsSourceConfig, numPartitions: Int, connector: JmsConnector) extends Table with SupportsRead {

  override def name(): String = {
    Seq(
      "jms",
      Utils.removeNonWordChars(connector.brokerName),
      Utils.removeNonWordChars(config.queueName),
    ).mkString("_")
  }

  override def schema(): StructType = JmsTable.DefaultSchema

  override def columns(): Array[Column] = JmsTable.DefaultColumns

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ)
  }

  // TODO: implement toContinuousStream
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = { () =>
    new Scan {
      override def readSchema(): StructType = JmsTable.DefaultSchema

      override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
        new JmsMicroBatchStream(config, numPartitions, checkpointLocation, connector)
      }
    }
  }
}

object JmsTable {

  private val DefaultSchema: StructType = StructType(
    Array(
      StructField("id", StringType),
      StructField("sent", LongType),
      StructField("received", LongType),
      StructField("value", BinaryType),
    )
  )

  private val DefaultColumns: Array[Column] = DefaultSchema.fields.map { field =>
    Column.create(field.name, field.dataType, field.nullable)
  }
}
