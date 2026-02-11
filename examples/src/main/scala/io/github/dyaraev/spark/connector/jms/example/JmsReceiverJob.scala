package io.github.dyaraev.spark.connector.jms.example

import io.github.dyaraev.spark.connector.jms.common.config.{JmsConnectionConfig, JmsSourceConfig, MessageFormat}
import io.github.dyaraev.spark.connector.jms.example.JmsReceiverJob.JmsReceiverJobConfig
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqBroker.ActiveMqAddress
import io.github.dyaraev.spark.connector.jms.example.utils.{CsvValueGenerator, FieldSpec}
import io.github.dyaraev.spark.connector.jms.provider.activemq.ActiveMqConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import java.nio.file.Path
import java.time.LocalDateTime
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

/**
 * Runs a Spark Structured Streaming job that consumes messages from a JMS queue and writes them to the filesystem in
 * the Parquet format
 */
class JmsReceiverJob(config: JmsReceiverJobConfig) {

  def runQueryAndWait(spark: SparkSession): Try[Unit] = {
    FieldSpec.toStructType(JmsReceiverJob.CsvFieldSpecs).flatMap { csvSchema =>
      Try {
        val query = runQuery(spark, csvSchema)
        query.awaitTermination()
      }
    }
  }

  private def runQuery(spark: SparkSession, csvSchema: StructType): StreamingQuery = {
    spark.readStream
      .format(config.sourceFormat)
      .option(JmsConnectionConfig.OptionProvider, ActiveMqConfig.ProviderName)
      .option(JmsConnectionConfig.OptionQueue, config.queueName)
      .option(JmsSourceConfig.OptionMessageFormat, MessageFormat.TextFormat.name)
      .option(JmsSourceConfig.OptionCommitIntervalMs, config.commitInterval.toMillis)
      .option(JmsSourceConfig.OptionNumPartitions, config.numPartitions.toString)
      .option(ActiveMqConfig.OptionsJmsBrokerUrl, config.brokerAddress.toString)
      .load()
      .withColumn("csv", from_csv(col("value"), csvSchema, Map.empty[String, String]))
      .select(Seq(col("id"), col("sent"), col("received")) ++ csvSchema.fieldNames.map(col("csv").getField): _*)
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

  private val CsvFieldSpecs: List[FieldSpec] = List(
    FieldSpec(CsvValueGenerator.withLogic("generator_id", (runId, _) => Success(runId)), StringType),
    FieldSpec(CsvValueGenerator.withLogic("row_num", (_, rowNum) => Success(rowNum.toString)), IntegerType),
    FieldSpec(CsvValueGenerator.oneOf("animal", "animals.txt"), StringType),
    FieldSpec(CsvValueGenerator.oneOf("color", "colors.txt"), StringType),
    FieldSpec(CsvValueGenerator.randomInt("count", bound = 100), IntegerType),
    FieldSpec(CsvValueGenerator.randomDateTime("datetime", min = LocalDateTime.now().minusDays(30)), StringType),
  )

  def apply(config: JmsReceiverJobConfig): JmsReceiverJob = new JmsReceiverJob(config)

  def generatorFields: Try[List[CsvValueGenerator]] = FieldSpec.fieldGenerators(CsvFieldSpecs)

  case class JmsReceiverJobConfig(
      outputPath: Path,
      checkpointPath: Path,
      sourceFormat: String,
      brokerAddress: ActiveMqAddress,
      queueName: String,
      numPartitions: Int,
      receiveTimeout: Duration,
      commitInterval: Duration,
      processingTime: Duration,
  )
}
