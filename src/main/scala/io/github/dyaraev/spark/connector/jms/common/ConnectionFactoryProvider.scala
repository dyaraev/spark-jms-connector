package io.github.dyaraev.spark.connector.jms.common

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap
import jakarta.jms.ConnectionFactory

import scala.util.{Failure, Success, Try}

trait ConnectionFactoryProvider {

  def brokerName: String

  def getConnectionFactory(options: CaseInsensitiveConfigMap): ConnectionFactory
}

object ConnectionFactoryProvider {

  def createInstance(className: String): ConnectionFactoryProvider = {
    def errorMessage: String = s"Unable to find implementation of JmsConnector by name: $className"

    Try(Class.forName(className).getDeclaredConstructor().newInstance()) match {
      case Success(c: ConnectionFactoryProvider) => c
      case Success(_)                            => throw new RuntimeException(errorMessage)
      case Failure(e)                            => throw new RuntimeException(errorMessage, e)
    }
  }
}
