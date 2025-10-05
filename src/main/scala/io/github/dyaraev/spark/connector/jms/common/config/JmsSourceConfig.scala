package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._

final case class JmsSourceConfig(
    connection: JmsConnectionConfig,
    messageFormat: MessageFormat,
    logIntervalMs: Long,
    bufferSize: Int,
    numOffsetsToKeep: Int,
    timeoutMs: Option[Long],
    messageSelector: Option[String],
    numPartitions: Option[Int],
)

//noinspection ScalaWeakerAccess
object JmsSourceConfig {

  val OptionMessageFormat = "jms.source.messageFormat"
  val OptionNumPartitions = "jms.source.numPartitions"
  val OptionNumOffsetsToKeep = "jms.source.numOffsetsToKeep"
  val OptionMessageSelector = "jms.source.messageSelector"
  val OptionLogIntervalMs = "jms.source.logIntervalMs"
  val OptionBufferSize = "jms.source.bufferSize"
  val OptionTimeoutMs = "jms.source.timeoutMs"

  private val DefaultBufferSize: Int = 5000
  private val DefaultLogIntervalMs: Long = 5000
  private val DefaultNumOffsetsToKeep: Int = 100

  def fromOptions(options: CaseInsensitiveConfigMap): JmsSourceConfig = {
    JmsSourceConfig(
      JmsConnectionConfig.fromOptions(options),
      options.getRequired[MessageFormat](OptionMessageFormat),
      options.getOptional[Long](OptionLogIntervalMs).getOrElse(DefaultLogIntervalMs),
      options.getOptional[Int](OptionBufferSize).getOrElse(DefaultBufferSize),
      options.getOptional[Int](OptionNumOffsetsToKeep).getOrElse(DefaultNumOffsetsToKeep),
      options.getOptional[Long](OptionTimeoutMs),
      options.getOptional[String](OptionMessageSelector),
      options.getOptional[Int](OptionNumPartitions),
    )
  }
}
