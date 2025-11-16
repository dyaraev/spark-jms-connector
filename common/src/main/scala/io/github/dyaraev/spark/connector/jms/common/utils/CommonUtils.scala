package io.github.dyaraev.spark.connector.jms.common.utils

import org.apache.spark.internal.Logging

import scala.util.control.NonFatal

object CommonUtils extends Logging {

  def logException(message: String): PartialFunction[Throwable, Unit] = { case NonFatal(e) =>
    logError(message, e)
  }

  def replaceNonWordChars(str: String, replacement: String): String = {
    str.replaceAll("\\W", replacement)
  }
}
