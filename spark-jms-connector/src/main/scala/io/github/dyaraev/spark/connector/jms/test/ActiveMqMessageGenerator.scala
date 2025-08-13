package io.github.dyaraev.spark.connector.jms.test

import jakarta.jms.JMSContext
import org.apache.activemq.{ActiveMQConnection, ActiveMQConnectionFactory}

import java.io.Closeable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object ActiveMqMessageGenerator {

  val QueueName = "spark.test.queue"

  private val RateMps = 20

  @volatile
  private var isStopped = true

  def main(args: Array[String]): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread("ShutdownHook") {
      override def run(): Unit = {
        println("Stopping application")
        isStopped = true
        Thread.sleep(2000)
      }
    })
    Await.ready(Future(generate()), Duration.Inf)
  }

  private def generate(): Unit = {
    println("Starting JMS message generator ...")
    val mqClient = ActiveMqClient()

    if (isStopped) {
      isStopped = false
      println("JMS message generator started")
      var counter = 1L
      val firstExecutionTime = System.currentTimeMillis()
      while (!isStopped) {
        try {
          mqClient.sendTextMessage(counter.toString)
          println(counter)
        } catch {
          case e: Exception => e.printStackTrace()
        }
        val delay = 1000L * counter / RateMps - (System.currentTimeMillis() - firstExecutionTime)
        if (delay > 0) Thread.sleep(delay)
        counter = counter + 1
      }
    }
    println("Stopping JMS message generator ...")
    mqClient.close()
    println("JMS message generator stopped")
  }

  private class ActiveMqClient(uri: String, queueName: String) extends Closeable {

    private val (connection, session, producer) = {
      val factory = new ActiveMQConnectionFactory(uri)
      val connection = factory.createConnection().asInstanceOf[ActiveMQConnection]
      val session = connection.createSession(false, JMSContext.CLIENT_ACKNOWLEDGE)
      val queue = session.createQueue(queueName)
      val producer = session.createProducer(queue)
      (connection, session, producer)
    }

    def sendTextMessage(text: String): Unit = {
      connection.start()
      val message = session.createTextMessage(text)
      producer.send(message)
    }

//    def sendBinaryMessage(data: Array[Byte]): Unit = {
//      val message = session.createBytesMessage()
//      message.writeBytes(data)
//      producer.send(message)
//    }

    def close(): Unit = {
      connection.close()
    }
  }

  private object ActiveMqClient {

    def apply(): ActiveMqClient = new ActiveMqClient(ActiveMqServer.Uri, QueueName)
  }
}
