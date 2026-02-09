package io.github.dyaraev.spark.connector.jms.common

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap
import jakarta.jms.ConnectionFactory
import org.apache.spark.internal.Logging

import java.util.{Locale, ServiceLoader}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

trait ConnectionFactoryProvider {

  /**
   * Provider identifier used to resolve providers via the SPI registry.
   */
  def name: String

  /**
   * Build a JMS connection factory using the provided connector options.
   *
   * @param options
   *   All options passed with the `jms.connection.broker.` prefix.
   * @return
   *   ConnectionFactory
   */
  def getConnectionFactory(options: CaseInsensitiveConfigMap): ConnectionFactory
}

object ConnectionFactoryProvider extends Logging {

  /**
   * Resolve a provider by broker name via [[ServiceLoader]] using the thread context [[ClassLoader]].
   *
   * Throws a [[RuntimeException]] if no matching provider is available.
   */
  def createInstanceByBrokerName(brokerName: String): ConnectionFactoryProvider = {
    try {
      val classLoader = Thread.currentThread().getContextClassLoader
      val providersMapping = loadProviders(classLoader)
      findProviderByBrokerName(providersMapping, brokerName)
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Unable to find ConnectionFactoryProvider by broker name '$brokerName'", e)
    }
  }

  /**
   * Load providers via SPI and validate broker name uniqueness and format.
   */
  private def loadProviders(classLoader: ClassLoader): Map[String, ConnectionFactoryProvider] = {
    def hasInvalidName(n: String): Boolean = n.isEmpty || !n.matches("^[A-Za-z][A-Za-z0-9_-]{2,}$")

    val providers = ServiceLoader
      .load(classOf[ConnectionFactoryProvider], classLoader)
      .iterator()
      .asScala
      .toList

    val invalid = providers.map(_.name).filter(hasInvalidName)
    if (invalid.nonEmpty) {
      val invalidNames = formatBrokerNamesList(invalid)
      throw new RuntimeException(s"Invalid broker name(s): $invalidNames")
    }

    val providersByName = providers.groupBy(_.name.toLowerCase(Locale.ROOT))
    val duplicates = providersByName.filter(_._2.length > 1).keys
    if (duplicates.nonEmpty) {
      val duplicateNames = formatBrokerNamesList(duplicates.toList)
      throw new RuntimeException(s"Duplicate broker name(s) registered: $duplicateNames")
    }

    providersByName.map { case (n, ps) => (n, ps.head) }
  }

  /**
   * Resolve a provider by normalized broker name or throw with available values.
   */
  private def findProviderByBrokerName(
      providers: Map[String, ConnectionFactoryProvider],
      brokerName: String,
  ): ConnectionFactoryProvider = {
    providers.get(brokerName.toLowerCase(Locale.ROOT)) match {
      case Some(provider) =>
        logInfo(s"Loaded JMS connection provider: ${provider.name}")
        provider
      case None =>
        val availableNames = formatBrokerNamesList(providers.keys.toList)
        throw new RuntimeException(s"Cannot resolve a ConnectionFactoryProvider (available brokers: $availableNames)")
    }
  }

  /**
   * Render a sorted list of broker names for error messages.
   */
  private def formatBrokerNamesList(brokerNames: List[String]): String = {
    brokerNames.map(n => s"'$n'").sorted.mkString("[", ", ", "]")
  }
}
