package io.github.dyaraev.spark.connector.jms.utils

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {

  private val sparkConf = new SparkConf()
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
//    .set("spark.rpc.message.maxSize", "512") // Default is 128 MB, increase to 512 MB
//    .set("spark.driver.maxResultSize", "2g")

  def getOrCreateSession(master: String = "local[2]", writeChecksumFiles: Boolean = false): SparkSession = {
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .master(master)
      .getOrCreate()

    if (!writeChecksumFiles) disableChecksumFiles(spark)
    spark
  }

  private def disableChecksumFiles(spark: SparkSession): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    fs.setWriteChecksum(false)
  }
}
