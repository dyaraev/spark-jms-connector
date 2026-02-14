package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.MapValueDecoder
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Locale
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

/**
 * Case-insensitive view over a string-to-string configuration map.
 *
 * Keys are normalized to lower case for lookups and equality checks while values keep their original case.
 *
 * @param original
 *   Original options map with case-sensitive keys.
 */
class CaseInsensitiveConfigMap(val original: Map[String, String]) extends Serializable {

  private val ciMap: Map[String, String] = original.map { case (k, v) => k.toLowerCase(Locale.ROOT) -> v }

  /**
   * Fetch an optional value and decode it to the requested type.
   *
   * @param key
   *   Lookup key (case-insensitive).
   * @tparam T
   *   Target type.
   * @return
   *   Decoded value or None when the key is missing.
   */
  def getOptional[T: ClassTag](key: String)(implicit decoder: MapValueDecoder[T]): Option[T] = {
    val maybeValue = ciMap.get(key.toLowerCase(Locale.ROOT))
    try maybeValue.map(decoder.decode)
    catch {
      case NonFatal(e) =>
        val className = classTag[T].runtimeClass.getName
        throw new RuntimeException(s"Unable to parse $className from $key", e)
    }
  }

  /**
   * Fetch a required value and decode it to the requested type.
   *
   * @param key
   *   Lookup key (case-insensitive).
   * @tparam T
   *   Target type.
   * @return
   *   Decoded value.
   */
  def getRequired[T: ClassTag](key: String)(implicit decoder: MapValueDecoder[T]): T = {
    val value = ciMap(key.toLowerCase(Locale.ROOT))
    try decoder.decode(value)
    catch {
      case NonFatal(e) =>
        val className = classTag[T].runtimeClass.getName
        throw new RuntimeException(s"Unable to parse $className from $key", e)
    }
  }

  /**
   * Create a new map that contains entries whose keys start with the provided prefix.
   *
   * @param prefix
   *   Prefix to filter with (case-insensitive).
   * @return
   *   Filtered map with original key/value pairs.
   */
  def withPrefix(prefix: String): CaseInsensitiveConfigMap = {
    val lcPrefix = prefix.toLowerCase(Locale.ROOT)
    val modifiedOriginalMap = original.filter { case (k, _) => k.toLowerCase(Locale.ROOT).startsWith(lcPrefix) }
    new CaseInsensitiveConfigMap(modifiedOriginalMap)
  }

  override def equals(other: Any): Boolean = other match {
    case that: CaseInsensitiveConfigMap => this.ciMap == that.ciMap
    case _                              => false
  }

  override def hashCode(): Int = ciMap.hashCode()
}

object CaseInsensitiveConfigMap {

  /**
   * Create a config map from a Scala Map.
   */
  def apply(original: Map[String, String]): CaseInsensitiveConfigMap = new CaseInsensitiveConfigMap(original)

  /**
   * Create a config map from a Java Map.
   */
  def apply(original: java.util.Map[String, String]): CaseInsensitiveConfigMap = {
    val mapBuilder = Map.newBuilder[String, String]
    original.entrySet().stream().forEach(e => mapBuilder += (e.getKey -> e.getValue))
    new CaseInsensitiveConfigMap(mapBuilder.result())
  }

  /**
   * Decoder for map values into typed values.
   */
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

    implicit class CaseInsensitiveStringMapOps(private val map: CaseInsensitiveStringMap) extends AnyVal {
      def toConfigMap: CaseInsensitiveConfigMap = CaseInsensitiveConfigMap(map.asCaseSensitiveMap())
    }
  }
}
