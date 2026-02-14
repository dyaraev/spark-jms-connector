package io.github.dyaraev.spark.connector.jms.common.client

import jakarta.jms._
import org.scalamock.scalatest.MockFactory
import org.scalamock.util.Defaultable
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.{Collections, Enumeration => JEnumeration}

class JmsSinkClientSpec extends AnyFunSuite with Matchers with MockFactory {

  implicit def defaultEnumeration[A]: Defaultable[JEnumeration[A]] = new Defaultable[java.util.Enumeration[A]] {
    override val default: JEnumeration[A] = Collections.emptyEnumeration[A]()
  }

  test("apply should start connection and use transacted session mode") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    recording.started shouldBe true
    recording.sessionMode shouldBe Some(Session.SESSION_TRANSACTED)
    client.close()
    recording.closed shouldBe true
  }

  test("sendTextMessage should create and send a text message") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    client.sendTextMessage("hello")
    recording.sentText shouldBe List("hello")
    client.close()
    recording.closed shouldBe true
  }

  test("sendBytesMessage should create and send a bytes message") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    val payload = Array[Byte](1, 2, 3)
    client.sendBytesMessage(payload)
    recording.sentBytes.size shouldBe 1
    recording.sentBytes.head.sameElements(payload) shouldBe true
    client.close()
    recording.closed shouldBe true
  }

  test("commit should call session.commit()") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    client.commit()
    recording.committed shouldBe 1
    client.close()
    recording.closed shouldBe true
  }

  test("rollback should delegate to session.rollback()") {
    val recording = new Recording
    val client = buildTransactedClient(recording)
    client.rollback()
    recording.rolledBack shouldBe 1
    client.close()
    recording.closed shouldBe true
  }

  test("closeSilently should swallow close exceptions") {
    val recording = new Recording(closeThrows = true)
    val client = buildTransactedClient(recording)
    noException should be thrownBy client.closeSilently()
  }

  private def buildTransactedClient(recording: Recording): JmsSinkClient = {
    val producer = stub[MessageProducer]
    (producer.send(_: Message)).when(*).onCall { message: Message =>
      message match {
        case text: TextMessage => recording.sentText = recording.sentText :+ text.getText
        case _: BytesMessage   =>
        case _                 =>
      }
    }

    val queue = stub[Queue]
    val session = stub[Session]
    (session.createQueue(_: String)).when("queue").returns(queue)
    (session.createProducer(_: Destination)).when(queue).returns(producer)

    val bytesMessage = stub[BytesMessage]
    (bytesMessage.writeBytes(_: Array[Byte])).when(*).onCall { bytes: Array[Byte] =>
      recording.sentBytes = recording.sentBytes :+ bytes
    }

    (() => session.createBytesMessage).when().returns(bytesMessage)
    (session.createTextMessage(_: String)).when(*).onCall { text: String =>
      val textMessage = stub[TextMessage]
      (() => textMessage.getText).when().returns(text)
      textMessage
    }

    (() => session.commit()).when().onCall { () => recording.committed += 1 }
    (() => session.rollback()).when().onCall { () => recording.rolledBack += 1 }

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

    JmsSinkClient(factory, "queue", None, None)
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
