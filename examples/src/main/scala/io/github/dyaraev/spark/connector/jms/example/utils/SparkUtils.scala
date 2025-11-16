package io.github.dyaraev.spark.connector.jms.example.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

object SparkUtils {

  private val sparkConf = new SparkConf()
//    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
//    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  def withSparkSession[T](master: String = "local[2]")(f: SparkSession => Try[T]): Try[T] = {
    getOrCreateSession(master).flatMap { spark =>
      val result = Try(f(spark).get)
      Try(spark.stop())
      result
    }
  }

  private def getOrCreateSession(master: String): Try[SparkSession] = Try {
    SparkSession
      .builder()
      .config(sparkConf)
      .master(master)
      .getOrCreate()
  }
}
