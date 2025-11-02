package io.github.dyaraev.spark.connector.jms.common.utils

import org.apache.spark.internal.Logging

object CommonUtils extends Logging {

  def logException(message: String): PartialFunction[Throwable, Unit] = { case e: Throwable =>
    logError(message, e)
  }

  def replaceNonWordChars(str: String, replacement: String): String = {
    str.replaceAll("\\W", replacement)
  }
}
