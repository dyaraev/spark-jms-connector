package io.github.dyaraev.spark.connector.jms.example.utils

import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqServer.{DefaultHost, DefaultPort}
import org.apache.activemq.broker.BrokerService
import org.apache.spark.internal.Logging

import java.nio.file.{Path, Paths}
import scala.reflect.io.Directory

trait ActiveMqServer extends Logging {

  lazy val Uri: String = s"$DefaultHost:$port"

  private lazy val brokerService = new BrokerService()

  protected def port: Int = DefaultPort

  protected def startServer(): Unit = {
    startServer(defaultDataDirectory)
  }

  protected def startServer(dataDirectory: Path): Unit = {
    logInfo("Starting ActiveMQ server ...")
    logInfo(s"ActiveMQ data directory: $dataDirectory")
    val dir = new Directory(dataDirectory.toFile)
    dir.deleteRecursively()
    brokerService.setDataDirectory(dataDirectory.toString)
    brokerService.addConnector(Uri)
    brokerService.start()
    logInfo("ActiveMQ server started")
  }

  protected def stopServer(): Unit = {
    logInfo("Stopping ActiveMQ server ...")
    brokerService.stop()
    logInfo("ActiveMQ server stopped")
  }

  private def defaultDataDirectory: Path = {
    Paths.get(getClass.getClassLoader.getResource(".").toURI).resolve("activemq-data")
  }
}

object ActiveMqServer {

  val DefaultHost = "tcp://localhost"
  val DefaultPort = 61616
}
