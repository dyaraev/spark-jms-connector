package io.github.dyaraev.spark.connector.jms.test

import org.apache.activemq.broker.BrokerService

import java.io.File
import scala.reflect.io.Directory

object ActiveMqServer {

  val Uri = "tcp://localhost:61616"

  private val dataDirectory = new File(new File(getClass.getClassLoader.getResource(".").toURI), "activemq-data")

  private lazy val brokerService = {
    val broker = new BrokerService()
    broker.setDataDirectory(dataDirectory.toString)
    broker.addConnector(Uri)
    broker
  }

  def main(args: Array[String]): Unit = {
    ActiveMqServer.start()
    println("ActiveMQ server started")

    // let's wait to avoid the JVM terminating
    ActiveMqServer.synchronized {
      ActiveMqServer.wait()
    }
  }

  def start(): Unit = {
    val dir = new Directory(dataDirectory)
    dir.deleteRecursively()
    brokerService.start()
  }

  def stop(): Unit = {
    brokerService.stop()
  }
}
