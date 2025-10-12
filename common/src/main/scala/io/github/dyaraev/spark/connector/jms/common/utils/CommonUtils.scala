package io.github.dyaraev.spark.connector.jms.common.utils

import org.apache.spark.internal.Logging

object CommonUtils extends Logging {

  def logException(message: String): PartialFunction[Throwable, Unit] = { case e: Throwable =>
    logError(message, e)
  }

  def replaceNonWordChars(str: String, replacement: String): String = {
    str.replaceAll("\\W", replacement)
  }

  def withCloseable[T <: AutoCloseable, R](closeable: => T)(processor: T => R): R = {
    var resource: T = null.asInstanceOf[T]
    try {
      resource = closeable
      processor(resource)
    } finally {
      try resource.close()
      catch logException("Unable to close resource")
    }
  }
}
