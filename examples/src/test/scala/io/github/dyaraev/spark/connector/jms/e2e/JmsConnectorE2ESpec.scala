package io.github.dyaraev.spark.connector.jms.e2e

import io.github.dyaraev.spark.connector.jms.common.client.{JmsSinkClient, JmsSourceClient}
import io.github.dyaraev.spark.connector.jms.common.config._
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqBroker
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqBroker.ActiveMqAddress
import io.github.dyaraev.spark.connector.jms.provider.activemq.ActiveMqConfig
import jakarta.jms.TextMessage
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.funsuite.AnyFunSuite

import java.net.ServerSocket
import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Try

class JmsConnectorE2ESpec extends AnyFunSuite {

  test("v1 sink writes messages to ActiveMQ") {
    runSinkE2eTest(format = "jms-v1", queueName = "e2e-sink-v1")
  }

  test("v2 sink writes messages to ActiveMQ") {
    runSinkE2eTest(format = "jms-v2", queueName = "e2e-sink-v2")
  }

  test("v1 source reads messages from ActiveMQ") {
    runSourceE2eTest(format = "jms-v1", queueName = "e2e-source-v1")
  }

  test("v2 source reads messages from ActiveMQ") {
    runSourceE2eTest(format = "jms-v2", queueName = "e2e-source-v2")
  }

  private def runSinkE2eTest(format: String, queueName: String): Unit = {
    val expectedMin = 10
    withTempDir(s"test-$format-$queueName") { tempDir =>
      withBroker(tempDir.resolve("amq")) { address =>
        withSparkSession(s"e2e-sink-$format") { spark =>
          val query = spark.readStream
            .format("rate")
            .option("rowsPerSecond", "10")
            .option("numPartitions", "1")
            .load()
            .select(functions.col("value").cast("string").as("value"))
            .writeStream
            .format(format)
            .option(JmsConnectionConfig.OptionProvider, ActiveMqConfig.ProviderName)
            .option(JmsConnectionConfig.OptionQueue, queueName)
            .option(JmsSinkConfig.OptionMessageFormat, MessageFormat.TextFormat.name)
            .option(ActiveMqConfig.OptionsJmsBrokerUrl, address.toString)
            .option("checkpointLocation", tempDir.resolve("checkpoint").toString)
            .trigger(Trigger.ProcessingTime(1.second))
            .start()

          try {
            waitForProgress(query, expectedMin = expectedMin.toLong, timeout = 20.seconds)
          } finally {
            query.stop()
            query.awaitTermination(10.seconds.toMillis)
          }

          val received = receiveBeforeDeadline(address, queueName, expectedMin = expectedMin, timeout = 10.seconds)
          assert(received.length >= expectedMin, "Expected at least 10 message from sink")
        }
      }
    }
  }

  private def runSourceE2eTest(format: String, queueName: String): Unit = {
    withTempDir(s"test-$format-$queueName") { tempDir =>
      withBroker(tempDir.resolve("amq")) { address =>
        val messages = List("message_1", "MESSAGE_1", "message_2", "MESSAGE_2")
        sendTextMessages(address, queueName, messages)

        withSparkSession(s"e2e-source-$format") { spark =>
          val queryName = Seq("memory", format, queueName).map(_.replace('-', '_')).mkString("_")
          val query = spark.readStream
            .format(format)
            .option(JmsConnectionConfig.OptionProvider, ActiveMqConfig.ProviderName)
            .option(JmsConnectionConfig.OptionQueue, queueName)
            .option(JmsSourceConfig.OptionMessageFormat, MessageFormat.TextFormat.name)
            .option(JmsSourceConfig.OptionCommitIntervalMs, "200")
            .option(JmsSourceConfig.OptionReceiveTimeoutMs, "200")
            .option(JmsSourceConfig.OptionNumPartitions, "1")
            .option(ActiveMqConfig.OptionsJmsBrokerUrl, address.toString)
            .load()
            .writeStream
            .format("memory")
            .queryName(queryName)
            .outputMode("append")
            .option("checkpointLocation", tempDir.resolve("checkpoint").toString)
            .trigger(Trigger.ProcessingTime(1.second))
            .start()

          try {
            waitForRowCount(spark, queryName, expected = messages.size.toLong, timeout = 20.seconds)
          } finally {
            query.stop()
            query.awaitTermination(10.seconds.toMillis)
          }

          import spark.implicits._
          val values = spark.table(queryName).select("value").as[String].collect().toList
          assert(
            values.toSet == messages.toSet,
            s"Expected values ${messages.mkString(",")} but got ${values.mkString(",")}",
          )
        }
      }
    }
  }

  private def withTempDir(prefix: String)(f: Path => Unit): Unit = {
    val dir = Files.createTempDirectory(prefix)
    try f(dir)
    finally deleteRecursively(dir)
  }

  private def withBroker(dataDir: Path)(f: ActiveMqAddress => Unit): Unit = {
    val address = ActiveMqAddress(freePort)
    val result = ActiveMqBroker.withActiveMqBroker(address, dataDir) { () =>
      Try(f(address))
    }
    result.failed.foreach(throw _)
  }

  private def withSparkSession(appName: String)(f: SparkSession => Unit): Unit = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try f(spark)
    finally spark.stop()
  }

  // noinspection SameParameterValue
  private def waitForProgress(query: StreamingQuery, expectedMin: Long, timeout: FiniteDuration): Unit = {
    val currentProgress = () => {
      val progress = query.lastProgress
      if (progress != null) progress.numInputRows
      else 0
    }

    val deadline = System.currentTimeMillis() + timeout.toMillis
    val count = progressBeforeDeadline(deadline, expectedMin, currentProgress)
    if (count < expectedMin) fail(s"Streaming query did not process $expectedMin rows within $timeout")
  }

  private def waitForRowCount(spark: SparkSession, tableName: String, expected: Long, timeout: FiniteDuration): Unit = {
    val currentProgress = () => {
      if (spark.catalog.tableExists(tableName)) spark.table(tableName).count()
      else 0
    }

    val deadline = System.currentTimeMillis() + timeout.toMillis
    val count = progressBeforeDeadline(deadline, expected, currentProgress)
    if (count < expected) fail(s"Expected at least $expected rows in $tableName but found $count")
  }

  @tailrec
  private def progressBeforeDeadline(deadline: Long, expectedMin: Long, currentProgress: () => Long): Long = {
    Thread.sleep(200)
    if (System.currentTimeMillis() < deadline) {
      val progress = currentProgress()
      if (progress >= expectedMin) progress
      else progressBeforeDeadline(deadline, expectedMin, currentProgress)
    } else {
      0
    }
  }

  private def sendTextMessages(address: ActiveMqAddress, queueName: String, messages: List[String]): Unit = {
    val config = connectionConfig(address, queueName)
    val client = JmsSinkClient(config, transacted = true)
    try {
      messages.foreach(client.sendTextMessage)
      client.commit()
    } finally {
      client.closeSilently()
    }
  }

  // noinspection SameParameterValue
  private def receiveBeforeDeadline(
      address: ActiveMqAddress,
      queueName: String,
      expectedMin: Int,
      timeout: FiniteDuration,
  ): List[String] = {

    @tailrec
    def receive(deadline: Long, client: JmsSourceClient, acc: List[String] = List.empty): List[String] = {
      if (System.currentTimeMillis() >= deadline || acc.length >= expectedMin) {
        acc
      } else {
        client.receive(500) match {
          case msg: TextMessage => receive(deadline, client, msg.getText :: acc)
          case _                => receive(deadline, client, acc)
        }
      }
    }

    val config = connectionConfig(address, queueName)
    val client = JmsSourceClient(config, transacted = false)
    val deadline = System.currentTimeMillis() + timeout.toMillis
    try receive(deadline, client)
    finally client.closeSilently()
  }

  private def connectionConfig(address: ActiveMqAddress, queueName: String): JmsConnectionConfig = {
    JmsConnectionConfig(
      provider = ActiveMqConfig.ProviderName,
      queue = queueName,
      username = None,
      password = None,
      selector = None,
      brokerOptions = CaseInsensitiveConfigMap(Map(ActiveMqConfig.OptionsJmsBrokerUrl -> address.toString)),
    )
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try stream.sorted(Comparator.reverseOrder()).forEach(Files.delete _)
      finally stream.close()
    }
  }

  private def freePort: Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }
}
