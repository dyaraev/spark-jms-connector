package io.github.dyaraev.spark.connector.jms.activemq.utils

import com.typesafe.scalalogging.Logger
import io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils
import jakarta.jms.{Connection, JMSContext, MessageProducer, Session}
import org.apache.activemq.{ActiveMQConnection, ActiveMQConnectionFactory}

import java.io.Closeable

object ActiveMqMessageGenerator {

  val QueueName = "spark.test.source"

  private val logger = Logger(getClass.getName)

  private val RateMps: Double = 5
  private val PrintIntervalMs: Long = 2000

  @volatile
  private var isStopped = false

  def main(args: Array[String]): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread("ShutdownHook") {
      override def run(): Unit = {
        logger.info("Stopping JMS message generator ...")
        isStopped = true
        Thread.sleep(PrintIntervalMs + 1000)
      }
    })
    generateMessage()
    ActiveMqMessageGenerator.synchronized(ActiveMqMessageGenerator.wait())
  }

  private def generateMessage(): Unit = {
    logger.info("Starting JMS message generator ...")

    var counter = 0L
    val startTimeMs = System.currentTimeMillis()

    new Thread("LoggingTimer") {
      setDaemon(true)

      override def run(): Unit = {
        var printCounter = 0L
        while (true) {
          logger.info(counter.toString)
          printCounter += 1
          val delay = printCounter * PrintIntervalMs - (System.currentTimeMillis() - startTimeMs)
          if (delay > 0) Thread.sleep(delay)
        }
      }
    }.start()

    CommonUtils.withCloseable(ActiveMqClient()) { mqClient =>
      while (!isStopped) {
        counter += 1
        mqClient.sendTextMessage(counter.toString)
        val delay = scala.math.round(1000L * counter / RateMps - (System.currentTimeMillis() - startTimeMs))
        if (delay > 0) Thread.sleep(delay)
      }
    }

    logger.info("JMS message generator stopped")
  }

  private class ActiveMqClient(uri: String, queueName: String) extends Closeable {

    private val (connection, session, producer) = {
      val connection = createConnection
      val session = createSession(connection)
      val producer = createProducer(session)
      (connection, session, producer)
    }

    def sendTextMessage(text: String): Unit = {
      val message = session.createTextMessage(text)
      producer.send(message)
    }

//    def sendBinaryMessage(data: Array[Byte]): Unit = {
//      val message = session.createBytesMessage()
//      message.writeBytes(data)
//      producer.send(message)
//    }

    def close(): Unit = connection.close()

    private def createConnection: Connection = {
      val factory = new ActiveMQConnectionFactory(uri)
      val connection = factory.createConnection().asInstanceOf[ActiveMQConnection]
      connection.setExceptionListener { case e: Throwable => e.printStackTrace() }
      connection.start()
      connection
    }

    private def createSession(connection: Connection): Session = {
      connection.createSession(false, JMSContext.CLIENT_ACKNOWLEDGE)
    }

    private def createProducer(session: Session): MessageProducer = {
      val queue = session.createQueue(queueName)
      session.createProducer(queue)
    }
  }

  private object ActiveMqClient {

    def apply(): ActiveMqClient = new ActiveMqClient(ActiveMqServer.Uri, QueueName)
  }
}
