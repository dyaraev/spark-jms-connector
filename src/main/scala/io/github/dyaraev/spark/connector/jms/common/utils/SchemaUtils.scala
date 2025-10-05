package io.github.dyaraev.spark.connector.jms.common.utils

import org.apache.spark.sql.connector.catalog.Column
import org.apache.spark.sql.types._

// TODO: remove
object SchemaUtils {

  // simplified version of the conversion function which doesn't process metadata
  def schemaToColumns(schema: StructType): Array[Column] = {
    schema.fields.map { field =>
      Column.create(field.name, field.dataType, field.nullable, field.getComment().orNull, field.metadata.json)
    }
  }

  // simplified version of the conversion function which doesn't process metadata
  def columnsToSchema(columns: Array[Column]): StructType = {
    StructType(columns.map { col =>
      val metadata = Option(col.metadataInJSON()).map(Metadata.fromJson).getOrElse(Metadata.empty)
      StructField(col.name(), col.dataType(), col.nullable(), metadata)
    })
  }
}
