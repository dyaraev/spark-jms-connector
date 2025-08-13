package io.github.dyaraev.spark.connector.jms.utils

import org.apache.spark.internal.Logging

object Utils extends Logging {

  def removeNonWordChars(str: String): String = str.replaceAll("\\W", "")

  def withCloseable[T <: AutoCloseable, R](closeable: => T)(processor: T => R): R = {
    var resource: T = null.asInstanceOf[T]
    try {
      resource = closeable
      processor(resource)
    } finally {
      try resource.close()
      catch {
        case e: Throwable => logError(s"Unable to close resource", e)
      }
    }
  }
}
