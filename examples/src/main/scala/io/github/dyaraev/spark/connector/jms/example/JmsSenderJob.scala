package io.github.dyaraev.spark.connector.jms.example

import io.github.dyaraev.spark.connector.jms.common.config.{JmsConnectionConfig, JmsSinkConfig, MessageFormat}
import io.github.dyaraev.spark.connector.jms.example.JmsSenderJob.JmsSenderJobConfig
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqBroker.ActiveMqAddress
import io.github.dyaraev.spark.connector.jms.provider.activemq.{ActiveMqConfig, ActiveMqConnectionFactoryProvider}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.StructType

import java.nio.file.Path
import scala.concurrent.duration.Duration
import scala.util.Try

class JmsSenderJob(schema: StructType, config: JmsSenderJobConfig) {

  def runQueryAndWait(spark: SparkSession): Try[Unit] = Try {
    val query = runQuery(spark)
    query.awaitTermination()
  }

  private def runQuery(spark: SparkSession): StreamingQuery = {
    spark.readStream
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load(config.inputPath.toString)
      .withColumn("value", concat_ws("-", col("color"), col("animal"), col("number")))
      .repartition(1)
      .writeStream
      .format(config.sinkFormat)
      .option(JmsSinkConfig.OptionMessageFormat, MessageFormat.TextFormat.name)
      .option(JmsConnectionConfig.OptionQueueName, config.queueName)
      .option(JmsConnectionConfig.OptionFactoryProvider, classOf[ActiveMqConnectionFactoryProvider].getName)
      .option(ActiveMqConfig.OptionsJmsBrokerAddress, config.brokerAddress.toString)
      .trigger(Trigger.ProcessingTime(config.processingTime.toMillis))
      .option("checkpointLocation", config.checkpointPath.toString)
      .queryName(JmsSenderJob.QueryName)
      .start()
  }
}

object JmsSenderJob {

  private val QueryName: String = "jms-sender-job"

  def apply(schema: StructType, config: JmsSenderJobConfig): JmsSenderJob = new JmsSenderJob(schema, config)

  case class JmsSenderJobConfig(
      inputPath: Path,
      checkpointPath: Path,
      sinkFormat: String,
      brokerAddress: ActiveMqAddress,
      queueName: String,
      processingTime: Duration,
  )
}
