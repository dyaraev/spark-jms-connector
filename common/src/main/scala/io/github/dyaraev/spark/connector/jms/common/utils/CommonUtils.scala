package io.github.dyaraev.spark.connector.jms.common.utils

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

import scala.util.Try
import scala.util.control.NonFatal

object CommonUtils extends Logging {

  def logException(message: String): PartialFunction[Throwable, Unit] = { case NonFatal(e) =>
    logError(message, e)
  }

  def formatMessage(message: String, batchId: Option[Long] = None): String = {
    val partitionId = Try(TaskContext.getPartitionId()).toOption
    val params = Seq(batchId.map("batchId=" + _), partitionId.map("partitionId=" + _)).flatten
    if (params.isEmpty) message
    else s"""$message [${params.mkString(", ")}]"""
  }

  def replaceNonWordChars(str: String, replacement: String): String = {
    str.replaceAll("\\W", replacement)
  }
}
