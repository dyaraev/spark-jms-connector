package io.github.dyaraev.spark.connector.jms.example

import com.typesafe.scalalogging.Logger
import io.github.dyaraev.spark.connector.jms.activemq.{ActiveMqConfig, ActiveMqConnectionFactoryProvider}
import io.github.dyaraev.spark.connector.jms.common.config.{JmsConnectionConfig, JmsSinkConfig, MessageFormat}
import io.github.dyaraev.spark.connector.jms.example.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.Trigger

import java.nio.file.Paths

// Your IDE amy require the following JVM option for running this code:
// --add-exports java.base/sun.nio.ch=ALL-UNNAMED
object JmsSinkExample extends Logging {

  private val logger = Logger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getOrCreateSession()

    val workingDirectory = Paths.get(args.head)
    val checkpointPath = workingDirectory.resolve("checkpoint")
    logger.info(s"Storing checkpoint data at $checkpointPath")

    val sourceDirectory = workingDirectory.resolve("incoming")
    logger.info(s"Reading data from $sourceDirectory")

    val query = spark.readStream
      .format("text")
      .load(sourceDirectory.toString)
      .repartition(1)
      .writeStream
      .format("jms-v2")
      .option(JmsSinkConfig.OptionMessageFormat, MessageFormat.TextFormat.name)
      .option(JmsConnectionConfig.OptionQueueName, ActiveMqMessageReader.QueueName)
      .option(JmsConnectionConfig.OptionFactoryProvider, classOf[ActiveMqConnectionFactoryProvider].getName)
      .option(ActiveMqConfig.OptionsJmsBrokerUrl, ActiveMqMessageReader.Uri)
      .trigger(Trigger.ProcessingTime(10000))
      .option("checkpointLocation", checkpointPath.toString)
      .start()

    query.awaitTermination()
  }
}
