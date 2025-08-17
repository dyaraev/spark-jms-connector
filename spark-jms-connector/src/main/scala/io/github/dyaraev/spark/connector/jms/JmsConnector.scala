package io.github.dyaraev.spark.connector.jms

import jakarta.jms.ConnectionFactory
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait JmsConnector extends Serializable {

  def getConnectionFactory(options: CaseInsensitiveStringMap): ConnectionFactory

  def brokerName: String
}

// TODO: make pluggable
object JmsConnector {

  def findImplementation(className: String): JmsConnector = Class.forName(className) match {
    case c: Class[JmsConnector] => c.getDeclaredConstructor().newInstance()
    case _ => throw new RuntimeException(s"Unable to find implementation of ConnectionFactoryProvider for $className")
  }
}
