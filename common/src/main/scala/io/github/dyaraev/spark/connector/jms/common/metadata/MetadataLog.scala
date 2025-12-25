package io.github.dyaraev.spark.connector.jms.common.metadata

import org.apache.spark.sql.SparkSession

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}
import scala.reflect.ClassTag
import scala.util.Using

// TODO: consider a better way to serialize/deserialize logs
class MetadataLog[T <: LogEntry: ClassTag](spark: SparkSession, checkpointLocation: String)
    extends SparkMetadataLog[T](spark, checkpointLocation) {

  override protected def serialize(metadata: Array[T], out: OutputStream): Unit = {
    Using.resource(new ObjectOutputStream(out))(_.writeObject(metadata))
  }

  override protected def deserialize(in: InputStream): Array[T] = {
    Using.resource(new ObjectInputStream(in))(_.readObject().asInstanceOf[Array[T]])
  }
}
