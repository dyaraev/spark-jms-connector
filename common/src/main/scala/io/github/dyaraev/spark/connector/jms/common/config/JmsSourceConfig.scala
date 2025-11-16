package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._

final case class JmsSourceConfig(
    connection: JmsConnectionConfig,
    messageFormat: MessageFormat,
    commitIntervalMs: Long,
    bufferSize: Int,
    numOffsetsToKeep: Int,
    receiveTimeoutMs: Option[Long],
    numPartitions: Option[Int],
) {

  def validate(): Unit = {
    require(commitIntervalMs > 0, "Commit interval must be positive")
    require(bufferSize > 0, "Buffer size must be positive")
    require(numOffsetsToKeep > 0, "Number of offsets to keep must be positive")
    numPartitions.foreach(p => require(p > 0, "Number of partitions must be positive"))
  }
}

//noinspection ScalaWeakerAccess
object JmsSourceConfig {

  val OptionBufferSize = "jms.source.bufferSize"
  val OptionCommitIntervalMs = "jms.source.commitIntervalMs"
  val OptionMessageFormat = "jms.source.messageFormat"
  val OptionMessageSelector = "jms.source.messageSelector"
  val OptionNumOffsetsToKeep = "jms.source.numOffsetsToKeep"
  val OptionNumPartitions = "jms.source.numPartitions"
  val OptionReceiveTimeoutMs = "jms.source.receiveTimeoutMs"

  val DefaultBufferSize: Int = 5000
  val DefaultCommitIntervalMs: Long = 5000
  val DefaultNumOffsetsToKeep: Int = 100

  def fromOptions(options: CaseInsensitiveConfigMap): JmsSourceConfig = {
    val config = JmsSourceConfig(
      JmsConnectionConfig.fromOptions(options),
      options.getRequired[MessageFormat](OptionMessageFormat),
      options.getOptional[Long](OptionCommitIntervalMs).getOrElse(DefaultCommitIntervalMs),
      options.getOptional[Int](OptionBufferSize).getOrElse(DefaultBufferSize),
      options.getOptional[Int](OptionNumOffsetsToKeep).getOrElse(DefaultNumOffsetsToKeep),
      options.getOptional[Long](OptionReceiveTimeoutMs),
      options.getOptional[Int](OptionNumPartitions),
    )
    config.validate()
    config
  }
}
