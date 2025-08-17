package io.github.dyaraev.spark.connector.jms

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class JmsSourceProvider extends SimpleTableProvider with DataSourceRegister with Logging {

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val config = JmsSourceConfig.fromOptions(options)
    val partitions = options.getInt("numPartitions", SparkSession.active.sparkContext.defaultParallelism)
    val connectionFactoryProvider = JmsConnector.findImplementation(config.receiver.connector)
    new JmsTable(config, partitions, connectionFactoryProvider)
  }

  override def shortName(): String = "jms"
}
