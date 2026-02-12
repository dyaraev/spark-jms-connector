package io.github.dyaraev.spark.connector.jms.common.client

import jakarta.jms._
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.{Level, LogManager}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.{Method, Proxy}

class JmsSinkClientSpec extends AnyFunSuite with Matchers {

  test("apply should start connection and use transacted session mode") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    recording.started shouldBe true
    recording.sessionMode shouldBe Some(Session.SESSION_TRANSACTED)
    client.close()
  }

  test("sendTextMessage should create and send a text message") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    client.sendTextMessage("hello")
    recording.sentText shouldBe List("hello")
    client.close()
  }

  test("sendBytesMessage should create and send a bytes message") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    val payload = Array[Byte](1, 2, 3)
    client.sendBytesMessage(payload)
    recording.sentBytes.size shouldBe 1
    recording.sentBytes.head.sameElements(payload) shouldBe true
    client.close()
  }

  test("commit and rollback should delegate to session") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    client.commit()
    client.rollback()
    recording.committed shouldBe 1
    recording.rolledBack shouldBe 1
    client.close()
  }

  test("closeSilently should swallow close exceptions") {
    val recording = new Recording(closeThrows = true)
    val client = buildTransactedClient(recording)
    withLogLevels(Seq("CommonUtils", "io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils"), Level.OFF) {
      noException should be thrownBy client.closeSilently()
    }
  }

  private def buildTransactedClient(recording: Recording): JmsSinkClient = {
    val producer = newProxy(classOf[MessageProducer]) { (method, args) =>
      method.getName match {
        case "send" =>
          val message = args.head.asInstanceOf[Message]
          message match {
            case text: TextMessage =>
              recording.sentText = recording.sentText :+ text.getText
            case _: BytesMessage =>
            case _               =>
          }
          null
        case _ => defaultReturn(method)
      }
    }

    val session = newProxy(classOf[Session]) { (method, args) =>
      method.getName match {
        case "createQueue" =>
          newProxy(classOf[Queue]) { (m, _) => defaultReturn(m) }
        case "createProducer" =>
          producer
        case "createBytesMessage" =>
          newProxy(classOf[BytesMessage]) { (m, a) =>
            m.getName match {
              case "writeBytes" =>
                val bytes = a.head.asInstanceOf[Array[Byte]]
                recording.sentBytes = recording.sentBytes :+ bytes
                null
              case _ => defaultReturn(m)
            }
          }
        case "createTextMessage" =>
          val text = args.head.asInstanceOf[String]
          newProxy(classOf[TextMessage]) { (m, _) =>
            m.getName match {
              case "getText" => text
              case _         => defaultReturn(m)
            }
          }
        case "commit" =>
          recording.committed += 1
          null
        case "rollback" =>
          recording.rolledBack += 1
          null
        case _ => defaultReturn(method)
      }
    }

    val connection = newProxy(classOf[Connection]) { (method, args) =>
      method.getName match {
        case "createSession" =>
          recording.sessionMode = Some(args.head.asInstanceOf[Integer].toInt)
          session
        case "start" =>
          recording.started = true
          null
        case "close" =>
          recording.closed = true
          if (recording.closeThrows) throw new RuntimeException("close failed")
          null
        case _ => defaultReturn(method)
      }
    }

    val factory = newProxy(classOf[ConnectionFactory]) { (method, _) =>
      method.getName match {
        case "createConnection" =>
          connection
        case _ => defaultReturn(method)
      }
    }

    JmsSinkClient(factory, "queue", None, None)
  }

  private def newProxy[T](iface: Class[T])(handler: (Method, Array[AnyRef]) => AnyRef): T = {
    Proxy
      .newProxyInstance(
        iface.getClassLoader,
        Array[Class[_]](iface),
        (_, method: Method, args: Array[AnyRef]) => handler(method, if (args == null) Array.empty[AnyRef] else args),
      )
      .asInstanceOf[T]
  }

  private def defaultReturn(method: Method): AnyRef = {
    val returnType = method.getReturnType
    if (!returnType.isPrimitive) {
      null
    } else if (returnType == java.lang.Boolean.TYPE) {
      java.lang.Boolean.FALSE
    } else if (returnType == java.lang.Void.TYPE) {
      null
    } else {
      Integer.valueOf(0)
    }
  }

  private def withLogLevels(names: Seq[String], level: Level)(f: => Unit): Unit = {
    val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config = ctx.getConfiguration
    val configs = names.map { name =>
      val loggerConfig = config.getLoggerConfig(name)
      val previous = loggerConfig.getLevel
      loggerConfig.setLevel(level)
      (loggerConfig, previous)
    }
    ctx.updateLoggers()
    try f
    finally {
      configs.foreach { case (loggerConfig, previous) => loggerConfig.setLevel(previous) }
      ctx.updateLoggers()
    }
  }

  final private class Recording(
      var sessionMode: Option[Int] = None,
      var started: Boolean = false,
      var closed: Boolean = false,
      var committed: Int = 0,
      var rolledBack: Int = 0,
      var sentText: List[String] = Nil,
      var sentBytes: List[Array[Byte]] = Nil,
      var closeThrows: Boolean = false,
  )
}
