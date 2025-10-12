package io.github.dyaraev.spark.connector.jms.common.config

sealed trait MessageFormat extends Serializable {

  def name: String

  override def toString: String = name
}

object MessageFormat {

  def apply(s: String): MessageFormat = s match {
    case TextFormat.name   => TextFormat
    case BinaryFormat.name => BinaryFormat
    case _                 => throw new RuntimeException(s"Unsupported message format '$s'")
  }

  object TextFormat extends MessageFormat {
    override val name: String = "text"
  }

  object BinaryFormat extends MessageFormat {
    override val name: String = "binary"
  }
}
