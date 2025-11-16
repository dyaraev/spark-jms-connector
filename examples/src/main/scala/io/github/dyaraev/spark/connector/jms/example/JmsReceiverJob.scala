package io.github.dyaraev.spark.connector.jms.example

import io.github.dyaraev.spark.connector.jms.common.config.{JmsConnectionConfig, JmsSourceConfig, MessageFormat}
import io.github.dyaraev.spark.connector.jms.example.JmsReceiverJob.JmsReceiverJobConfig
import io.github.dyaraev.spark.connector.jms.example.activemq.{ActiveMqConfig, ActiveMqConnectionFactoryProvider}
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqBroker.ActiveMqAddress
import io.github.dyaraev.spark.connector.jms.example.utils.Implicits.LetSyntax
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.nio.file.Path
import scala.concurrent.duration.Duration
import scala.util.Try

class JmsReceiverJob(config: JmsReceiverJobConfig) {

  def runQueryAndWait(spark: SparkSession): Try[Unit] = Try {
    val query = runQuery(spark)
    query.awaitTermination()
  }

  private def runQuery(spark: SparkSession): StreamingQuery = {
    spark.readStream
      .format(config.sourceFormat)
      .option(JmsConnectionConfig.OptionQueueName, config.queueName)
      .option(JmsConnectionConfig.OptionFactoryProvider, classOf[ActiveMqConnectionFactoryProvider].getName)
      .option(JmsSourceConfig.OptionMessageFormat, MessageFormat.TextFormat.name)
      .option(JmsSourceConfig.OptionCommitIntervalMs, config.commitInterval.toMillis)
      .option(JmsSourceConfig.OptionNumPartitions, config.numPartitions.toString)
      .let(r => config.receiveTimeout.fold(r)(t => r.option(JmsSourceConfig.OptionReceiveTimeoutMs, t.toMillis)))
      .option(ActiveMqConfig.OptionsJmsBrokerAddress, config.brokerAddress.toString)
      .load()
      .repartition(1)
      .writeStream
      .format("parquet")
      .option("checkpointLocation", config.checkpointPath.toString)
      .trigger(Trigger.ProcessingTime(config.processingTime.toMillis))
      .queryName(JmsReceiverJob.QueryName)
      .start(config.outputPath.toString)
  }
}

object JmsReceiverJob {

  private val QueryName: String = "jms-receiver-job"

  def apply(config: JmsReceiverJobConfig): JmsReceiverJob = new JmsReceiverJob(config)

  case class JmsReceiverJobConfig(
      outputPath: Path,
      checkpointPath: Path,
      sourceFormat: String,
      brokerAddress: ActiveMqAddress,
      queueName: String,
      numPartitions: Int,
      receiveTimeout: Option[Duration],
      commitInterval: Duration,
      processingTime: Duration,
  )
}
