package io.github.dyaraev.spark.connector.jms

import io.github.dyaraev.spark.connector.jms.activemq.utils.{ActiveMqMessageGenerator, ActiveMqServer}
import io.github.dyaraev.spark.connector.jms.activemq.{ActiveMqConfig, ActiveMqConnectionFactoryProvider}
import io.github.dyaraev.spark.connector.jms.config.{JmsConnectionConfig, JmsSourceConfig, MessageFormat}
import io.github.dyaraev.spark.connector.jms.utils.SparkUtils
import org.apache.spark.sql.streaming.Trigger

import java.nio.file.Paths

object TestJmsSource {

  private val WorkingDirectory = "/Users/dyaraev/Temp/jms-source"
  private val OutputTableName = "test_jms_table"

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getOrCreateSession()

    val workingDirectoryPath = Paths.get(WorkingDirectory)
    val outputPath = workingDirectoryPath.resolve("output")
    val checkpointPath = workingDirectoryPath.resolve("checkpoint")

    val query = spark.readStream
      .format("jms-v2")
      .option(JmsConnectionConfig.OptionQueueName, ActiveMqMessageGenerator.QueueName)
      .option(JmsConnectionConfig.OptionFactoryProvider, classOf[ActiveMqConnectionFactoryProvider].getName)
      .option(JmsSourceConfig.OptionMessageFormat, MessageFormat.TextFormat.name)
      .option(JmsSourceConfig.OptionLogIntervalMs, "10000")
      .option(JmsSourceConfig.OptionTimeoutMs, "1000")
      .option(JmsSourceConfig.OptionNumPartitions, 8)
      .option(ActiveMqConfig.OptionsJmsBrokerUrl, ActiveMqServer.Uri)
      .load()
      .repartition(1)
      .writeStream
      .format("delta")
      .option("path", outputPath.toString)
      .option("checkpointLocation", checkpointPath.toString)
      .trigger(Trigger.ProcessingTime(10000))
      .toTable(OutputTableName)

    query.awaitTermination()
  }
}
