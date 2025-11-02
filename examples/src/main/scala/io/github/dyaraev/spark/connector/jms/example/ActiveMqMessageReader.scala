package io.github.dyaraev.spark.connector.jms.example

import io.github.dyaraev.spark.connector.jms.common.client.JmsSourceClient
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqServer
import jakarta.jms.{BytesMessage, TextMessage}
import org.apache.activemq.ActiveMQConnectionFactory

import java.nio.file.Paths
import scala.util.Using

object ActiveMqMessageReader extends ActiveMqServer {

  val QueueName: String = "spark.test.sink"

  @volatile
  private var isStopped: Boolean = false

  def main(args: Array[String]): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread("ShutdownHook") {
      override def run(): Unit = {
        stopConsumer()
        stopServer()
      }
    })

    args.headOption.map(Paths.get(_)) match {
      case Some(dataDirectory) => startServer(dataDirectory)
      case None                => startServer()
    }

    startConsumer()
    ActiveMqMessageReader.synchronized(ActiveMqMessageReader.wait())
  }

  override protected def port: Int = 61617

  private def startConsumer(): Unit = {
    val factory = new ActiveMQConnectionFactory(Uri)
    val client = JmsSourceClient(factory, QueueName)

    logInfo("Starting JMS message reader ...")
    val consumerThread = new Thread("MessageReader") {
      setDaemon(true)

      override def run(): Unit = {
        Using(client) { c =>
          while (!isStopped) {
            c.receive(1000) match {
              case message: BytesMessage =>
                val length = message.getBodyLength.toInt
                val buffer = new Array[Byte](length)
                message.readBytes(buffer)
                logInfo(s"BYTES ($length): ${new String(buffer)}")
              case message: TextMessage =>
                val text = message.getText
                val length = text.length
                logInfo(s"TEXT ($length): ${text.take(20)}")
              case null =>
            }
          }
        }
      }
    }
    consumerThread.start()
  }

  private def stopConsumer(): Unit = {
    logInfo("Stopping JMS message reader ...")
    isStopped = true
    Thread.sleep(2000)
    logInfo("JMS message reader stopped")
  }
}
