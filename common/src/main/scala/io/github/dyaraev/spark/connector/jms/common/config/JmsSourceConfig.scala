package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._

final case class JmsSourceConfig(
    connection: JmsConnectionConfig,
    messageFormat: MessageFormat,
    logIntervalMs: Long,
    bufferSize: Int,
    numOffsetsToKeep: Int,
    receiveTimeoutMs: Option[Long],
    numPartitions: Option[Int],
)

//noinspection ScalaWeakerAccess
object JmsSourceConfig {

  val OptionBufferSize = "jms.source.bufferSize"
  val OptionLogIntervalMs = "jms.source.logIntervalMs"
  val OptionMessageFormat = "jms.source.messageFormat"
  val OptionMessageSelector = "jms.source.messageSelector"
  val OptionNumOffsetsToKeep = "jms.source.numOffsetsToKeep"
  val OptionNumPartitions = "jms.source.numPartitions"
  val OptionReceiveTimeoutMs = "jms.source.receiveTimeoutMs"

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
      options.getOptional[Long](OptionReceiveTimeoutMs),
      options.getOptional[Int](OptionNumPartitions),
    )
  }
}
