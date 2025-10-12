package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.MapValueDecoder
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Locale
import scala.reflect.{ClassTag, classTag}

class CaseInsensitiveConfigMap(val original: Map[String, String]) extends Serializable {

  private val cimap: Map[String, String] = original.map { case (k, v) => k.toLowerCase(Locale.ROOT) -> v }

  def getOptional[T: ClassTag](key: String)(implicit decoder: MapValueDecoder[T]): Option[T] = {
    val maybeValue = cimap.get(key.toLowerCase(Locale.ROOT))
    try maybeValue.map(decoder.decode)
    catch {
      case e: Throwable =>
        val className = classTag[T].runtimeClass.getName
        throw new RuntimeException(s"Unable to parse $className from $key", e)
    }
  }

  def getRequired[T: ClassTag](key: String)(implicit decoder: MapValueDecoder[T]): T = {
    val value = cimap(key.toLowerCase(Locale.ROOT))
    try decoder.decode(value)
    catch {
      case e: Throwable =>
        val className = classTag[T].runtimeClass.getName
        throw new RuntimeException(s"Unable to parse $className from $key", e)
    }
  }

  def withPrefix(prefix: String): CaseInsensitiveConfigMap = {
    val lcPrefix = prefix.toLowerCase(Locale.ROOT)
    val modifiedOriginalMap = original.filterKeys(_.toLowerCase(Locale.ROOT).startsWith(lcPrefix)).map(identity)
    CaseInsensitiveConfigMap(modifiedOriginalMap)
  }
}

object CaseInsensitiveConfigMap {

  def apply(original: Map[String, String]): CaseInsensitiveConfigMap = new CaseInsensitiveConfigMap(original)

  def apply(original: java.util.Map[String, String]): CaseInsensitiveConfigMap = {
    val mapBuilder = Map.newBuilder[String, String]
    original.entrySet().stream().forEach(e => mapBuilder += (e.getKey -> e.getValue))
    new CaseInsensitiveConfigMap(mapBuilder.result())
  }

  trait MapValueDecoder[T] {
    def decode(v: String): T
  }

  object Implicits {

    implicit val BooleanMapValueDecoder: MapValueDecoder[Boolean] = (v: String) => v.trim.toBoolean
    implicit val DoubleMapValueDecoder: MapValueDecoder[Double] = (v: String) => v.trim.toDouble
    implicit val IntMapValueDecoder: MapValueDecoder[Int] = (v: String) => v.trim.toInt
    implicit val LongMapValueDecoder: MapValueDecoder[Long] = (v: String) => v.trim.toLong
    implicit val StringMapValueDecoder: MapValueDecoder[String] = (v: String) => v
    implicit val MessageFormatDecoder: MapValueDecoder[MessageFormat] = (s: String) => MessageFormat(s)

    implicit class CaseInsensitiveStringMapOps(val map: CaseInsensitiveStringMap) extends AnyVal {
      def toConfigMap: CaseInsensitiveConfigMap = CaseInsensitiveConfigMap(map.asCaseSensitiveMap())
    }
  }
}
