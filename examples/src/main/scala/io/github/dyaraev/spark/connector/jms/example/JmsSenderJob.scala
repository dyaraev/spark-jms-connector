package io.github.dyaraev.spark.connector.jms.example

import io.github.dyaraev.spark.connector.jms.common.config.{JmsConnectionConfig, JmsSinkConfig, MessageFormat}
import io.github.dyaraev.spark.connector.jms.example.JmsSenderJob.JmsSenderJobConfig
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqBroker.ActiveMqAddress
import io.github.dyaraev.spark.connector.jms.example.utils.{CsvValueGenerator, FieldSpec}
import io.github.dyaraev.spark.connector.jms.provider.activemq.ActiveMqConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import java.nio.file.Path
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

/**
 * Runs a Spark Structured Streaming job that ingests files from the filesystem and sends extracted records as messages
 * to a JMS queue
 */
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
      .withColumn("value", concat_ws("-", col("row_num"), col("color"), col("animal")))
      .repartition(1)
      .writeStream
      .format(config.sinkFormat)
      .option(JmsConnectionConfig.OptionProvider, ActiveMqConfig.ProviderName)
      .option(JmsConnectionConfig.OptionQueue, config.queueName)
      .option(JmsSinkConfig.OptionMessageFormat, MessageFormat.TextFormat.name)
      .option(ActiveMqConfig.OptionsJmsBrokerUrl, config.brokerAddress.toString)
      .trigger(Trigger.ProcessingTime(config.processingTime.toMillis))
      .option("checkpointLocation", config.checkpointPath.toString)
      .queryName(JmsSenderJob.QueryName)
      .start()
  }
}

object JmsSenderJob {

  private val QueryName: String = "jms-sender-job"

  val FieldSpecs: List[FieldSpec] = List(
    FieldSpec(CsvValueGenerator.withLogic("run_id", (runId, _) => Success(runId)), StringType),
    FieldSpec(CsvValueGenerator.withLogic("row_num", (_, rowNum) => Success(rowNum.toString)), IntegerType),
    FieldSpec(CsvValueGenerator.oneOf("animal", "animals.txt"), StringType),
    FieldSpec(CsvValueGenerator.oneOf("color", "colors.txt"), StringType),
  )

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
