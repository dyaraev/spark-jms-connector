package io.github.dyaraev.spark.connector.jms.common.metadata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.HDFSMetadataLog

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}
import scala.reflect.ClassTag
import scala.util.Using

// TODO: consider a better way to serialize/deserialize logs
class MetadataLog[T <: LogEntry: ClassTag](spark: SparkSession, checkpointLocation: String)
    extends HDFSMetadataLog[Array[T]](spark, checkpointLocation) {

  override protected def serialize(metadata: Array[T], out: OutputStream): Unit = {
    Using(new ObjectOutputStream(out))(_.writeObject(metadata)).get
  }

  override protected def deserialize(in: InputStream): Array[T] = {
    Using(new ObjectInputStream(in))(_.readObject().asInstanceOf[Array[T]]).get
  }
}
