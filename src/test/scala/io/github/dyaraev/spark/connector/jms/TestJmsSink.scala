package io.github.dyaraev.spark.connector.jms

import com.typesafe.scalalogging.Logger
import io.github.dyaraev.spark.connector.jms.activemq.utils.{ActiveMqMessageReader, ActiveMqServer}
import io.github.dyaraev.spark.connector.jms.activemq.{ActiveMqConfig, ActiveMqConnectionFactoryProvider}
import io.github.dyaraev.spark.connector.jms.config.{JmsConnectionConfig, JmsSinkConfig, MessageFormat}
import io.github.dyaraev.spark.connector.jms.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.Trigger

import java.nio.file.Paths

object TestJmsSink extends Logging {

  private val logger = Logger(getClass)
  private val WorkingDirectory = "/Users/dyaraev/Temp/jms-sink"

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getOrCreateSession()

    val workingDirectoryPath = Paths.get(WorkingDirectory)
    val checkpointPath = workingDirectoryPath.resolve("checkpoint")
    logger.info(s"Storing checkpoint data at $checkpointPath")

    val resourcePath = Paths.get(getClass.getClassLoader.getResource("source").toURI)
    logger.info(s"Reading data from $resourcePath")

    val query = spark.readStream
      .format("text")
      .load(resourcePath.toString)
      .repartition(1)
      .writeStream
      .format("jms-v2")
      .option(JmsSinkConfig.OptionMessageFormat, MessageFormat.TextFormat.name)
      .option(JmsConnectionConfig.OptionQueueName, ActiveMqMessageReader.QueueName)
      .option(JmsConnectionConfig.OptionFactoryProvider, classOf[ActiveMqConnectionFactoryProvider].getName)
      .option(ActiveMqConfig.OptionsJmsBrokerUrl, ActiveMqServer.Uri)
      .trigger(Trigger.ProcessingTime(10000))
      .option("checkpointLocation", checkpointPath.toString)
      .start()

    query.awaitTermination()
  }
}
