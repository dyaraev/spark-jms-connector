package io.github.dyaraev.spark.connector.jms.common

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap
import jakarta.jms.{Connection, ConnectionFactory, JMSContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class ConnectionFactoryProviderSpec extends AnyFunSuite with Matchers {

  test("createInstanceByBrokerName should resolve provider via thread context ClassLoader (case-insensitive)") {
    withServiceLoader(
      classOf[ConnectionFactoryProviderSpec.TestProviderAlpha].getName,
      classOf[ConnectionFactoryProviderSpec.TestProviderBeta].getName,
    ) {
      val provider = ConnectionFactoryProvider.createInstanceByBrokerName("ALPHA")
      provider shouldBe a[ConnectionFactoryProviderSpec.TestProviderAlpha]
    }
  }

  test("createInstanceByBrokerName should report missing broker") {
    withServiceLoader(classOf[ConnectionFactoryProviderSpec.TestProviderAlpha].getName) {
      val ex = intercept[RuntimeException] {
        ConnectionFactoryProvider.createInstanceByBrokerName("missing")
      }
      ex.getMessage should include("Unable to find ConnectionFactoryProvider by broker name 'missing'")
      ex.getCause.getMessage should include("Cannot resolve a ConnectionFactoryProvider")
    }
  }

  test("loadProviders should reject invalid broker names") {
    withServiceLoader(classOf[ConnectionFactoryProviderSpec.TestProviderInvalid].getName) {
      val ex = intercept[RuntimeException] {
        ConnectionFactoryProvider.createInstanceByBrokerName("anything")
      }
      ex.getMessage should include("Unable to find ConnectionFactoryProvider by broker name 'anything'")
      ex.getCause.getMessage should include("Invalid broker name(s)")
    }
  }

  test("loadProviders should reject invalid characters in broker names") {
    withServiceLoader(classOf[ConnectionFactoryProviderSpec.TestProviderInvalidChars].getName) {
      val ex = intercept[RuntimeException] {
        ConnectionFactoryProvider.createInstanceByBrokerName("anything")
      }
      ex.getMessage should include("Unable to find ConnectionFactoryProvider by broker name 'anything'")
      ex.getCause.getMessage should include("Invalid broker name(s)")
    }
  }

  test("loadProviders should reject duplicate broker names (case-insensitive)") {
    withServiceLoader(
      classOf[ConnectionFactoryProviderSpec.TestProviderAlpha].getName,
      classOf[ConnectionFactoryProviderSpec.TestProviderAlphaDuplicate].getName,
    ) {
      val ex = intercept[RuntimeException] {
        ConnectionFactoryProvider.createInstanceByBrokerName("alpha")
      }
      ex.getMessage should include("Unable to find ConnectionFactoryProvider by broker name 'alpha'")
      ex.getCause.getMessage should include("Duplicate broker name(s) registered")
    }
  }

  private def withServiceLoader(providerClasses: String*)(run: => Unit): Unit = {
    val tempDir = Files.createTempDirectory("cfp-service-loader")
    val serviceFile = writeServiceFile(tempDir, providerClasses.toList)
    val classLoader = new URLClassLoader(Array(tempDir.toUri.toURL), getClass.getClassLoader)
    val thread = Thread.currentThread()
    val original = thread.getContextClassLoader
    try {
      thread.setContextClassLoader(classLoader)
      run
    } finally {
      thread.setContextClassLoader(original)
      classLoader.close()
      Files.deleteIfExists(serviceFile)
    }
  }

  private def writeServiceFile(baseDir: Path, providerClasses: List[String]): Path = {
    val servicesDir = baseDir.resolve("META-INF/services")
    Files.createDirectories(servicesDir)
    val serviceFile = servicesDir.resolve(classOf[ConnectionFactoryProvider].getName)
    val contents = providerClasses.mkString("\n")
    Files.write(serviceFile, contents.getBytes(StandardCharsets.UTF_8))
    serviceFile
  }
}

object ConnectionFactoryProviderSpec {

  private class DummyConnectionFactory extends ConnectionFactory {

    override def createConnection(): Connection =
      throw new UnsupportedOperationException("test-only")

    override def createConnection(userName: String, password: String): Connection =
      throw new UnsupportedOperationException("test-only")

    override def createContext(): JMSContext =
      throw new UnsupportedOperationException("test-only")

    override def createContext(userName: String, password: String): JMSContext =
      throw new UnsupportedOperationException("test-only")

    override def createContext(sessionMode: Int): JMSContext =
      throw new UnsupportedOperationException("test-only")

    override def createContext(userName: String, password: String, sessionMode: Int): JMSContext =
      throw new UnsupportedOperationException("test-only")
  }

  trait TestConnectionFactoryProvider extends ConnectionFactoryProvider {
    override def getConnectionFactory(options: CaseInsensitiveConfigMap): ConnectionFactory = new DummyConnectionFactory
  }

  private class TestProviderAlpha extends TestConnectionFactoryProvider {
    override val brokerName: String = "alpha"
  }

  private class TestProviderBeta extends TestConnectionFactoryProvider {
    override val brokerName: String = "beta"
  }

  private class TestProviderAlphaDuplicate extends TestConnectionFactoryProvider {
    override val brokerName: String = "ALPHA"
  }

  private class TestProviderInvalid extends TestConnectionFactoryProvider {
    override val brokerName: String = "ab"
  }

  private class TestProviderInvalidChars extends TestConnectionFactoryProvider {
    override val brokerName: String = "a*b"
  }
}
