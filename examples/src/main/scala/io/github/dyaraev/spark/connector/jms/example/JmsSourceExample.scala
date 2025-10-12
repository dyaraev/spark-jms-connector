package io.github.dyaraev.spark.connector.jms.example

import io.github.dyaraev.spark.connector.jms.activemq.{ActiveMqConfig, ActiveMqConnectionFactoryProvider}
import io.github.dyaraev.spark.connector.jms.common.config.{JmsConnectionConfig, JmsSourceConfig, MessageFormat}
import io.github.dyaraev.spark.connector.jms.example.utils.SparkUtils
import org.apache.spark.sql.streaming.Trigger

import java.nio.file.Paths

// Your IDE amy require the following JVM option for running this code:
// --add-exports java.base/sun.nio.ch=ALL-UNNAMED
object JmsSourceExample {

  private val OutputTableName = "test_jms_table"

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getOrCreateSession()

    val workingDirectory = Paths.get(args.head)
    val outputPath = workingDirectory.resolve("output")
    val checkpointPath = workingDirectory.resolve("checkpoint")

    val query = spark.readStream
      .format("jms-v2")
      .option(JmsConnectionConfig.OptionQueueName, ActiveMqMessageGenerator.QueueName)
      .option(JmsConnectionConfig.OptionFactoryProvider, classOf[ActiveMqConnectionFactoryProvider].getName)
      .option(JmsSourceConfig.OptionMessageFormat, MessageFormat.TextFormat.name)
      .option(JmsSourceConfig.OptionLogIntervalMs, "10000")
      .option(JmsSourceConfig.OptionReceiveTimeoutMs, "1000")
      .option(JmsSourceConfig.OptionNumPartitions, 8)
      .option(ActiveMqConfig.OptionsJmsBrokerUrl, ActiveMqMessageGenerator.Uri)
      .load()
      .repartition(1)
      .writeStream
      .format("parquet")
      .option("path", outputPath.toString)
      .option("checkpointLocation", checkpointPath.toString)
      .trigger(Trigger.ProcessingTime(10000))
      .toTable(OutputTableName)

    query.awaitTermination()
  }
}
