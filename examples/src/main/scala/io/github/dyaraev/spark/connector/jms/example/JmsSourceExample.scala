package io.github.dyaraev.spark.connector.jms.example

import com.typesafe.scalalogging.Logger
import io.github.dyaraev.spark.connector.jms.activemq.{ActiveMqConfig, ActiveMqConnectionFactoryProvider}
import io.github.dyaraev.spark.connector.jms.common.config.{JmsConnectionConfig, JmsSourceConfig, MessageFormat}
import io.github.dyaraev.spark.connector.jms.example.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import java.nio.file.Paths

// Your IDE amy require the following JVM option for running this code:
// --add-opens java.base/sun.nio.ch=ALL-UNNAMED
object JmsSourceExample {

  private val logger = Logger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getOrCreateSession()
    Runtime.getRuntime.addShutdownHook {
      new Thread("ShutdownHook") {
        override def run(): Unit = {
          logger.info("Stopping Spark session ...")
          spark.stop()
        }
      }
    }

    val workingDirectory = Paths.get(args.head)
    val outputPath = workingDirectory.resolve("output")
    val checkpointPath = workingDirectory.resolve("checkpoint")
    logger.info(s"Writing data to $outputPath")
    logger.info(s"Storing checkpoint data at $checkpointPath")
    runJob(spark, outputPath.toString, checkpointPath.toString)
  }

  private def runJob(spark: SparkSession, outputPath: String, checkpointPath: String): Unit = {
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
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime(10000))
      .start(outputPath)

    query.awaitTermination()
  }
}
