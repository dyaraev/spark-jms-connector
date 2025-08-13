package io.github.dyaraev.spark.connector.jms

import io.github.dyaraev.spark.connector.jms.JmsSourceConfig.Implicits._
import io.github.dyaraev.spark.connector.jms.JmsSourceConfig.{BrokerConfig, ReceiverConfig}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.reflect.{ClassTag, classTag}

// TODO: make broker configuration pluggable
final case class JmsSourceConfig(
    queueName: String,
    messageSelector: Option[String],
    username: Option[String],
    password: Option[String],
    receiver: ReceiverConfig,
    broker: BrokerConfig,
)

object JmsSourceConfig {

  private val OptionJmsQueueName = "jms.queueName"
  private val OptionJmsMessageSelector = "jms.messageSelector"
  private val OptionJmsUsername = "jms.username"
  private val OptionJmsPassword = "jms.password"

  def fromOptions(options: CaseInsensitiveStringMap): JmsSourceConfig = {
    JmsSourceConfig(
      options.get(OptionJmsQueueName),
      options.getOptional[String](OptionJmsMessageSelector),
      options.getOptional[String](OptionJmsUsername),
      options.getOptional[String](OptionJmsPassword),
      ReceiverConfig.fromOptions(options),
      BrokerConfig.fromOptions(options),
    )
  }

  sealed trait BrokerConfig {

    def brokerType: String
  }

  object BrokerConfig {

    private val OptionJmsBrokerType = "jms.broker.type"

    def fromOptions(options: CaseInsensitiveStringMap): BrokerConfig = {
      options.get(OptionJmsBrokerType) match {
        case ActiveMqConfig.BrokerType => ActiveMqConfig.fromOptions(options)
        case another                   => throw new RuntimeException(s"Unknown broker type: $another")
      }
    }
  }

  // ActiveMQ configuration classes

  final case class ActiveMqConfig(url: String) extends BrokerConfig {
    override val brokerType: String = ActiveMqConfig.BrokerType
  }

  object ActiveMqConfig {

    val BrokerType = "active-mq"

    private val OptionsJmsBrokerUrl = "jms.broker.url"

    def fromOptions(options: CaseInsensitiveStringMap): ActiveMqConfig = {
      ActiveMqConfig(options.get(OptionsJmsBrokerUrl))
    }
  }

  // JMS receiver configuration

  case class ReceiverConfig(
      intervalMs: Long,
      bufferSize: Long = ReceiverConfig.DefaultBufferSize,
      timeoutMs: Option[Long],
  )

  object ReceiverConfig {

    private val DefaultBufferSize: Long = 5000

    private val OptionIntervalMs = "jms.receiver.intervalMs"
    private val OptionBufferSize = "jms.receiver.bufferSize"
    private val OptionTimeoutMs = "jms.receiver.timeoutMs"

    def fromOptions(options: CaseInsensitiveStringMap): ReceiverConfig = ReceiverConfig(
      options.getRequired[Long](OptionIntervalMs),
      options.getLong(OptionBufferSize, DefaultBufferSize),
      options.getOptional[Long](OptionTimeoutMs),
    )
  }

  // Utility methods and classes

  trait MapValueDecoder[T] {
    def decode(v: String): T
  }

  object Implicits {

    implicit val LongMapValueDecoder: MapValueDecoder[Long] = (v: String) => v.trim.toLong
    implicit val StringMapValueDecoder: MapValueDecoder[String] = (v: String) => v

    implicit class CaseInsensitiveStringMapOps(val map: CaseInsensitiveStringMap) extends AnyVal {

      def getOptional[T: ClassTag](key: String)(implicit decoder: MapValueDecoder[T]): Option[T] = {
        if (map.containsKey(key)) Some(getRequired(key))
        else None
      }

      def getRequired[T: ClassTag](key: String)(implicit decoder: MapValueDecoder[T]): T = {
        try decoder.decode(map.get(key))
        catch {
          case e: Throwable =>
            val className = classTag[T].runtimeClass.getName
            throw new RuntimeException(s"Unable to parse $className from $key", e)
        }
      }
    }
  }
}
