package io.github.dyaraev.spark.connector.jms.common.client

import jakarta.jms._
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.{Level, LogManager}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.{Method, Proxy}

class JmsSourceClientSpec extends AnyFunSuite with Matchers {

  test("apply should start connection and use transacted session mode") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    recording.started shouldBe true
    recording.sessionMode shouldBe Some(Session.SESSION_TRANSACTED)
    client.close()
  }

  test("receive should delegate to consumer") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    val message = newProxy(classOf[Message]) { (m, _) => defaultReturn(m) }
    recording.nextMessage = message
    client.receive(100L) shouldBe message
    recording.lastReceiveTimeout shouldBe Some(100L)
    client.close()
  }

  test("commit should delegate to session") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    client.commit()
    recording.committed shouldBe 1
    client.close()
  }

  test("closeSilently should swallow close exceptions") {
    val recording = new Recording(closeThrows = true)
    val client = buildTransactedClient(recording)
    withLogLevels(Seq("CommonUtils", "io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils"), Level.OFF) {
      noException should be thrownBy client.closeSilently()
    }
  }

  private def buildTransactedClient(recording: Recording): JmsSourceClient = {
    val consumer = newProxy(classOf[MessageConsumer]) { (method, args) =>
      method.getName match {
        case "receive" =>
          recording.lastReceiveTimeout = Some(args.head.asInstanceOf[java.lang.Long].longValue())
          recording.nextMessage
        case _ => defaultReturn(method)
      }
    }

    val session = newProxy(classOf[Session]) { (method, _) =>
      method.getName match {
        case "createQueue" =>
          newProxy(classOf[Queue]) { (m, _) => defaultReturn(m) }
        case "createConsumer" =>
          consumer
        case "commit" =>
          recording.committed += 1
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
        case "createConnection" => connection
        case _                  => defaultReturn(method)
      }
    }

    JmsSourceClient(factory, "queue", None, None, None)
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
      var closeThrows: Boolean = false,
      var nextMessage: Message = null,
      var lastReceiveTimeout: Option[Long] = None,
  )
}
