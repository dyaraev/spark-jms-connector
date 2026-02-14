package io.github.dyaraev.spark.connector.jms.common.client

import jakarta.jms._
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.{Level, LogManager}
import org.scalamock.scalatest.MockFactory
import org.scalamock.util.Defaultable
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.{Collections, Enumeration => JEnumeration}

class JmsSourceClientSpec extends AnyFunSuite with Matchers with MockFactory {

  implicit def defaultEnumeration[A]: Defaultable[JEnumeration[A]] = new Defaultable[java.util.Enumeration[A]] {
    override val default: JEnumeration[A] = Collections.emptyEnumeration[A]()
  }

  test("apply should start connection and use transacted session mode") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    recording.started shouldBe true
    recording.sessionMode shouldBe Some(Session.SESSION_TRANSACTED)
    client.close()
  }

  test("receive should delegate to consumer") {
    val recording = new Recording
    val message = stub[Message]
    val client = buildTransactedClient(recording)
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
    val consumer = stub[MessageConsumer]
    (consumer.receive(_: Long)).when(*).onCall { timeout: Long =>
      recording.lastReceiveTimeout = Some(timeout)
      recording.nextMessage
    }

    val queue = stub[Queue]
    val session = stub[Session]
    (session.createQueue(_: String)).when("queue").returns(queue)
    (session.createConsumer(_: Destination, _: String)).when(queue, *).returns(consumer)
    (() => session.commit()).when().onCall { () => recording.committed += 1 }

    val connection = stub[Connection]
    (connection.createSession(_: Int)).when(Session.SESSION_TRANSACTED).onCall { mode: Int =>
      recording.sessionMode = Some(mode)
      session
    }

    (() => connection.start()).when().onCall { () => recording.started = true }
    (() => connection.close()).when().onCall { () =>
      recording.closed = true
      if (recording.closeThrows) throw new RuntimeException("close failed")
    }

    val factory = stub[ConnectionFactory]
    (() => factory.createConnection).when().returns(connection)

    JmsSourceClient(factory, "queue", None, None, None)
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
