package io.github.dyaraev.spark.connector.jms.example.utils

import com.typesafe.scalalogging.Logger
import org.apache.activemq.broker.BrokerService

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Try

object ActiveMqBroker {

  private val logger = Logger(getClass)

  def withActiveMqBroker(address: ActiveMqAddress, dataDirectory: Path)(f: () => Try[Unit]): Try[Unit] = {
    deleteRecursively(dataDirectory)
    val broker = start(address, dataDirectory)
    val result = f()
    stop(broker)
    result
  }

  private def start(address: ActiveMqAddress, dataDirectory: Path): BrokerService = {
    logger.info(s"Starting ActiveMQ broker on $address ...")
    val broker = new BrokerService()
    broker.setBrokerName("EmbeddedBroker")
    broker.setUseShutdownHook(false)
    broker.setDeleteAllMessagesOnStartup(true)
    broker.setDataDirectory(dataDirectory.toString)
    broker.addConnector(address.toString)
    broker.start()
    broker.waitUntilStarted()
    logger.info("ActiveMQ broker started")
    broker
  }

  private def stop(broker: BrokerService): Unit = {
    logger.info(s"Stopping ActiveMQ broker ...")
    broker.stop()
    broker.waitUntilStopped()
    logger.info(s"ActiveMQ broker stopped")
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      logger.info(s"Cleaning up the ActiveMQ data directory $path")
      Files
        .walk(path)
        .iterator()
        .asScala
        .toList
        .reverse
        .foreach(Files.delete)
    }
  }

  case class ActiveMqAddress(port: Int) {

    private val host: String = "tcp://localhost"

    override def toString: String = s"$host:$port"
  }
}
