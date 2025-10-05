package io.github.dyaraev.spark.connector.jms.activemq.utils

import com.typesafe.scalalogging.Logger
import jakarta.jms.{BytesMessage, JMSContext, TextMessage}
import org.apache.activemq.{ActiveMQConnection, ActiveMQConnectionFactory}

object ActiveMqMessageReader {

  val QueueName = "spark.test.sink"

  private val logger = Logger(getClass.getName)

  @volatile
  private var isStopped = false

  def main(args: Array[String]): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread("ShutdownHook") {
      override def run(): Unit = {
        logger.info("Stopping JMS message reader ...")
        isStopped = true
        Thread.sleep(2000)
      }
    })
    logger.info("Starting JMS message reader ...")
    consumeMessage()
    ActiveMqMessageReader.synchronized(ActiveMqMessageReader.wait())
  }

  private def consumeMessage(): Unit = {
    val factory = new ActiveMQConnectionFactory(ActiveMqServer.Uri)
    val connection = factory.createConnection().asInstanceOf[ActiveMQConnection]
    connection.start()

    val session = connection.createSession(false, JMSContext.CLIENT_ACKNOWLEDGE)
    val queue = session.createQueue(QueueName)
    val consumer = session.createConsumer(queue)

    val consumerThread = new Thread("MqConsumerThread") {
      setDaemon(true)

      override def run(): Unit = {
        while (!isStopped) {
          consumer.receive(1000) match {
            case message: BytesMessage =>
              val buffer = new Array[Byte](message.getBodyLength.toInt)
              message.readBytes(buffer)
              logger.info(s"BYTES: ${new String(buffer)}")
            case message: TextMessage =>
              logger.info(s"TEXT: ${message.getText}")
            case null =>
          }
        }
      }
    }
    consumerThread.start()
  }
}
