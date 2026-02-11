package io.github.dyaraev.spark.connector.jms.example

import com.typesafe.scalalogging.Logger
import io.delta.tables.DeltaTable
import io.github.dyaraev.spark.connector.jms.common.config.{JmsConnectionConfig, JmsSourceConfig, MessageFormat}
import io.github.dyaraev.spark.connector.jms.example.JmsReceiverDeltaJob.JmsReceiverDeltaJobConfig
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqBroker.ActiveMqAddress
import io.github.dyaraev.spark.connector.jms.example.utils.Implicits.LetSyntax
import io.github.dyaraev.spark.connector.jms.example.utils.{CsvValueGenerator, FieldSpec}
import io.github.dyaraev.spark.connector.jms.provider.activemq.ActiveMqConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.nio.file.Path
import java.time.LocalDateTime
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

/**
 * Runs a Spark Structured Streaming job that consumes messages from a JMS queue and persists them into a Delta table
 * using the Merge operation.
 */
class JmsReceiverDeltaJob(config: JmsReceiverDeltaJobConfig) {

  def runQueryAndWait(spark: SparkSession): Try[Unit] = {
    FieldSpec.toStructType(JmsReceiverDeltaJob.CsvFieldSpecs).flatMap { csvSchema =>
      Try {
        val tableSchema = StructType(csvSchema.fields ++ JmsReceiverDeltaJob.JmsFields)
        val table = getOrCreateTable(spark, tableSchema, config.outputPath.toString)
        val query = runQuery(spark, csvSchema, table)
        query.awaitTermination()
      }
    }
  }

  private def runQuery(spark: SparkSession, csvSchema: StructType, table: DeltaTable): StreamingQuery = {
    spark.readStream
      .format(config.sourceFormat)
      .option(JmsConnectionConfig.OptionProvider, ActiveMqConfig.ProviderName)
      .option(JmsConnectionConfig.OptionQueue, config.queueName)
      .option(JmsSourceConfig.OptionMessageFormat, MessageFormat.TextFormat.name)
      .option(JmsSourceConfig.OptionCommitIntervalMs, config.commitInterval.toMillis)
      .option(JmsSourceConfig.OptionNumPartitions, config.numPartitions.toString)
      .option(JmsSourceConfig.OptionReceiveTimeoutMs, config.receiveTimeout.toMillis)
      .option(ActiveMqConfig.OptionsJmsBrokerUrl, config.brokerAddress.toString)
      .load()
      .withColumn("csv", from_csv(col("value"), csvSchema, Map.empty[String, String]))
      .withColumn("sent_dt", from_unixtime(col("sent") / 1000.0, "yyyy-MM-dd"))
      .let(csvSchema.fieldNames.foldLeft(_) { case (df, f) => df.withColumn(f, col("csv").getField(f)) })
      .writeStream
      .queryName(JmsReceiverDeltaJob.QueryName)
      .option("checkpointLocation", config.checkpointPath.toString)
      .trigger(Trigger.ProcessingTime(config.processingTime.toMillis))
      .foreachBatch { (df: DataFrame, _: Long) =>
        val ddf = df.dropDuplicates("id", "sent_dt") // real pipelines might require more complex deduplication logic
        insertMissing(table.as("t"), ddf.as("s"), col("t.sent_dt") === col("s.sent_dt") && col("t.id") === col("s.id"))
      }
      .start()
  }

  private def insertMissing(table: DeltaTable, df: DataFrame, condition: Column): Unit = {
    table
      .merge(df, condition)
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  private def getOrCreateTable(spark: SparkSession, schema: StructType, path: String): DeltaTable = {
    JmsReceiverDeltaJob.logger.info(s"Loading Delta table from path $path")
    if (DeltaTable.isDeltaTable(spark, path)) {
      DeltaTable.forPath(path)
    } else {
      DeltaTable
        .createOrReplace(spark)
        .tableName(JmsReceiverDeltaJob.DeltaTableName)
        .addColumns(schema)
        .partitionedBy("sent_dt")
        .location(path)
        .property("delta.autoOptimize.autoCompact", "true")
        .property("delta.autoOptimize.optimizeWrite", "true")
        .execute()
    }
  }
}

object JmsReceiverDeltaJob {

  private val logger = Logger(getClass)

  private val QueryName: String = "jms-receiver-delta-job"

  private val DeltaTableName: String = "jms_receiver_delta_table"

  private val JmsFields = Seq(
    StructField("id", StringType),
    StructField("sent", LongType),
    StructField("received", LongType),
    StructField("sent_dt", StringType),
  )

  private val CsvFieldSpecs: List[FieldSpec] = List(
    FieldSpec(CsvValueGenerator.withLogic("generator_id", (runId, _) => Success(runId)), StringType),
    FieldSpec(CsvValueGenerator.withLogic("row_num", (_, rowNum) => Success(rowNum.toString)), IntegerType),
    FieldSpec(CsvValueGenerator.oneOf("animal", "animals.txt"), StringType),
    FieldSpec(CsvValueGenerator.oneOf("color", "colors.txt"), StringType),
    FieldSpec(CsvValueGenerator.randomInt("count", bound = 100), IntegerType),
    FieldSpec(CsvValueGenerator.randomDateTime("datetime", min = LocalDateTime.now().minusDays(30)), StringType),
  )

  def apply(config: JmsReceiverDeltaJobConfig): JmsReceiverDeltaJob = new JmsReceiverDeltaJob(config)

  def generatorFields: Try[List[CsvValueGenerator]] = FieldSpec.fieldGenerators(CsvFieldSpecs)

  case class JmsReceiverDeltaJobConfig(
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
