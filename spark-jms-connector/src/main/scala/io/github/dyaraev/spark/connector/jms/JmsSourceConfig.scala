package io.github.dyaraev.spark.connector.jms

import io.github.dyaraev.spark.connector.jms.JmsSourceConfig.Implicits._
import io.github.dyaraev.spark.connector.jms.JmsSourceConfig.ReceiverConfig
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.reflect.{ClassTag, classTag}

final case class JmsSourceConfig(
    queueName: String,
    messageSelector: Option[String],
    username: Option[String],
    password: Option[String],
    receiver: ReceiverConfig,
    brokerOptions: CaseInsensitiveStringMap,
)

object JmsSourceConfig {

  private val OptionJmsQueueName = "jms.queueName"
  private val OptionJmsMessageSelector = "jms.messageSelector"
  private val OptionJmsUsername = "jms.username"
  private val OptionJmsPassword = "jms.password"

  private val BrokerOptionsPrefix = "jms.broker."

  def fromOptions(options: CaseInsensitiveStringMap): JmsSourceConfig = {

    JmsSourceConfig(
      options.get(OptionJmsQueueName),
      options.getOptional[String](OptionJmsMessageSelector),
      options.getOptional[String](OptionJmsUsername),
      options.getOptional[String](OptionJmsPassword),
      ReceiverConfig.fromOptions(options),
      options.subset(BrokerOptionsPrefix),
    )
  }

  // JMS receiver configuration

  case class ReceiverConfig(
      connector: String,
      intervalMs: Long,
      bufferSize: Long = ReceiverConfig.DefaultBufferSize,
      timeoutMs: Option[Long],
  )

  object ReceiverConfig {

    private val DefaultBufferSize: Long = 5000

    private val OptionConnector = "jms.receiver.connector"
    private val OptionIntervalMs = "jms.receiver.intervalMs"
    private val OptionBufferSize = "jms.receiver.bufferSize"
    private val OptionTimeoutMs = "jms.receiver.timeoutMs"

    def fromOptions(options: CaseInsensitiveStringMap): ReceiverConfig = ReceiverConfig(
      options.getRequired[String](OptionConnector),
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

    implicit val BooleanMapValueDecoder: MapValueDecoder[Boolean] = (v: String) => v.trim.toBoolean
    implicit val DoubleMapValueDecoder: MapValueDecoder[Double] = (v: String) => v.trim.toDouble
    implicit val IntMapValueDecoder: MapValueDecoder[Int] = (v: String) => v.trim.toInt
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

      def subset(prefix: String): CaseInsensitiveStringMap = {
        val lcPrefix = prefix.toLowerCase(Locale.ROOT)
        val filteredMap = map.asCaseSensitiveMap().asScala.filter {
          case (key, _) => key.toLowerCase(Locale.ROOT).startsWith(lcPrefix)
        }
        new CaseInsensitiveStringMap(filteredMap.asJava)
      }
    }
  }
}
