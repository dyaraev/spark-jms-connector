package io.github.dyaraev.spark.connector.jms.example

import io.github.dyaraev.spark.connector.jms.common.client.JmsSinkClient
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqServer
import org.apache.activemq.ActiveMQConnectionFactory

import java.nio.file.Paths
import scala.util.Using

object ActiveMqMessageGenerator extends ActiveMqServer {

  val QueueName: String = "spark.test.source"

  private val RateMps: Double = 5
  private val PrintIntervalMs: Long = 2000

  @volatile
  private var isStopped: Boolean = false

  @volatile
  private var counter: Long = 0

  def main(args: Array[String]): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread("ShutdownHook") {
      override def run(): Unit = {
        stopGenerator()
        stopServer()
      }
    })

    args.headOption.map(Paths.get(_)) match {
      case Some(dataDirectory) => startServer(dataDirectory)
      case None                => startServer()
    }

    startLogger()
    startGenerator()
    ActiveMqMessageGenerator.synchronized(ActiveMqMessageGenerator.wait())
  }

  override protected def port: Int = 61616

  private def startLogger(): Unit = {
    logInfo("Starting counter logger ...")
    val loggerThread = new Thread("LoggingTimer") {
      setDaemon(true)

      override def run(): Unit = {
        val startTimeMs = System.currentTimeMillis()
        var printCounter = 0L
        while (true) {
          logInfo(counter.toString)

          printCounter += 1
          val delay = printCounter * PrintIntervalMs - (System.currentTimeMillis() - startTimeMs)
          if (delay > 0) Thread.sleep(delay)
        }
      }
    }
    loggerThread.start()
  }

  private def startGenerator(): Unit = {
    val factory = new ActiveMQConnectionFactory(Uri)
    val client = JmsSinkClient(factory, QueueName)

    logInfo("Starting JMS message generator ...")
    val generatorThread = new Thread("MessageGenerator") {
      setDaemon(true)

      override def run(): Unit = {
        val startTimeMs = System.currentTimeMillis()
        Using(client) { mqClient =>
          while (!isStopped) {
            counter += 1
            mqClient.sendTextMessage(counter.toString)
            val delay = scala.math.round(1000L * counter / RateMps - (System.currentTimeMillis() - startTimeMs))
            if (delay > 0) Thread.sleep(delay)
          }
        }
      }
    }
    generatorThread.start()
  }

  private def stopGenerator(): Unit = {
    logInfo("Stopping JMS message generator ...")
    isStopped = true
    val delay = scala.math.max(PrintIntervalMs, scala.math.ceil(1000 / RateMps).toLong)
    Thread.sleep(delay + 2000)
    logInfo("JMS message generator stopped")
  }
}
