package io.github.dyaraev.spark.connector.jms

import io.github.dyaraev.spark.connector.jms.activemq.utils.{ActiveMqMessageGenerator, ActiveMqServer}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object TestJmsSource {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
//      .set("spark.rpc.message.maxSize", "512") // Default is 128 MB, increase to 512 MB
//      .set("spark.driver.maxResultSize", "2g")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .master("local[2]")
      .getOrCreate()

    disableChecksumFiles(spark)

    val query = spark.readStream
      .format("jms")
      .option("jms.queueName", ActiveMqMessageGenerator.QueueName)
      .option("jms.receiver.intervalMs", "5000")
      .option("jms.receiver.timeoutMs", "1000")
      .option("jms.receiver.connector", "io.github.dyaraev.spark.connector.jms.activemq.ActiveMqConnector")
      .option("jms.broker.url", ActiveMqServer.Uri)
      .option("numPartitions", 16)
      .load()
      .repartition(1)
      .writeStream
      .format("delta")
      .option("path", "/Users/dyaraev/Temp/jms-source/out")
      .option("checkpointLocation", "/Users/dyaraev/Temp/jms-source/checkpoint")
      .trigger(Trigger.ProcessingTime(10000))
      .toTable("test_jms_table")

    query.awaitTermination()
  }

  private def disableChecksumFiles(spark: SparkSession): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    fs.setWriteChecksum(false)
  }
}
