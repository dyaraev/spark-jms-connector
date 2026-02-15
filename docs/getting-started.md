# Getting Started

## Reading from JMS Queue

By default, the JMS source provides At-Least-Once delivery guarantee. 
Exactly-Once delivery can be achieved by using specific output formats, for instance, Delta Lake.

The resulting DataFrame schema returned by the source consists of the following fields:

- `StructField("id", StringType)` - JMS Message ID set by the broker.
- `StructField("sent", LongType)` - Timestamp when the message was sent to the broker.
- `StructField("received", LongType)` - Timestamp when the message was received by the connector.
- `StructField("value", <type>)` - Message content (see below for the type of the field).

The `value` field type depends on the `jms.messageFormat` option:

- When set to `text`: expects `TextMessage` and the value field is of type `StringType` (text content)
- When set to `binary`: expects `BytesMessage` and the value field is of type `BinaryType` (byte array)

This is an example of how to use the connector to read data from a JMS queue:

```scala
val df = spark.readStream
  .format("jms-v2") // or "jms-v1" depending on the used implementation
  .option("jms.connection.provider", "myProvider")
  .option("jms.connection.queue", "myQueue")
  .option("jms.messageFormat", "text")
  .option("jms.receiveTimeoutMs", "1000")
  .option("jms.commitIntervalMs", "10000")
  .option("jms.numPartitions", "8")
  .load()
```

## Writing to JMS Queue

The connector does not guarantee Exactly-Once delivery for sink operations. 
If a job fails after messages are committed, they will be re-delivered. 
To achieve Exactly-Once delivery guarantees, the target system should support idempotent writes.
Since we can only append to JMS queues, we can only talk about At-Least-Once semantics.

The input DataFrame must contain a field named `value` that holds the message content to be sent. The type of the field should be either `StringType` or `BinaryType`. The connector automatically handles type conversion based on the `jms.messageFormat` option:

/// html | div.format-table

| Field Type   | Message Format | JMS Message Type | Conversion Behavior                    |
|--------------|----------------|------------------|----------------------------------------|
| `StringType` | `text`         | `TextMessage`    | Sent as-is                             |
| `BinaryType` | `text`         | `TextMessage`    | `Array[Byte]` is converted to `String` |
| `BinaryType` | `binary`       | `BytesMessage`   | Sent as-is                             |
| `StringType` | `binary`       | `BytesMessage`   | `String` is converted to `Array[Byte]` |

///

This is an example that demonstrates how to use the connector to write data to a JMS queue:

```scala 
df.writeStream
  .format("jms-v2") // or "jms-v1" depending on the used implementation
  .option("jms.connection.provider", "myProvider")
  .option("jms.connection.queue", "myQueue")
  .option("jms.messageFormat", "text")
  .option("checkpointLocation", "/tmp/checkpoint")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()
  .awaitTermination()
```

It's important to note that the sink creates one JMS connection per partition when sending messages because each partition processes its data independently.
The implementation may not be optimal, as it does not use connection pooling when connecting to JMS brokers.

```scala
df.repartition(1) // Limit to 1 concurrent JMS connection
  .writeStream
  .format("jms-v2")
  // other options
  .start()
  .awaitTermination()
```

## Running Examples

The project includes examples for working with ActiveMQ. To run each example, you have to execute commands:

```bash
# Run an ActiveMQ broker, a test message generator and an example Spark job with the JMS source.
# The result is persisted into the filesystem in the Parquet format.
sbt "examples/runMain io.github.dyaraev.spark.connector.jms.example.ExampleApp receiver-job --workingDirectory /tmp/spark-receiver-job"
```

The example uses a message generator that sends text JMS messages with CSV payloads. 
The job reads the JMS queue into a stream, parses the CSV fields, keeps the JMS metadata (`id`, `sent`, `received`) plus the parsed columns, and writes the result to Parquet files.
There’s no deduplication, so it’s a straightforward At‑Least‑Once file sink.

```bash
# Run an ActiveMQ broker, a test message generator and an example Spark job with the JMS source.
# The result is persisted into a Delta table using MERGE.
sbt "examples/runMain io.github.dyaraev.spark.connector.jms.example.ExampleApp receiver-delta-job --workingDirectory /tmp/spark-receiver-delta-job"
```

As in the previous example, the source is CSV messages sent to a JMS queue by the generator. 
The job reads the stream, parses the CSV fields, adds a `sent_dt` date from the message timestamp, and writes into a Delta table partitioned by day. 
It uses JMS message ID represented by the `id` field and `sent_dt` as a composite merge key.
It also drops duplicates per batch and inserts missing rows using the MERGE. 
Assuming message IDs are unique for each day and the source is At‑Least‑Once, this gives Exactly‑Once results in the Delta table for that key.

```bash
# Run an ActiveMQ broker, a test file generator, an example Spark job with the JMS sink and a test message reader.
sbt "examples/runMain io.github.dyaraev.spark.connector.jms.example.ExampleApp sender-job --workingDirectory /tmp/spark-sender-job"
```

In this example, the job watches a folder for CSV files, parses rows with the provided schema, and builds a text payload from `row_num`, `color`, and `animal`.
It then writes those rows as JMS text messages to the target queue using the configured sink format. 
It also re-partitions data to one task per batch, so messages are sent from within a single partition.

You can provide Spark, Delta and Scala versions to the command in the same way as for [building the artifacts](development.md#building-the-project).
You can also play with the example applications by changing the configuration options.
All available options are listed when running commands with the `--help` flag.

Executing examples in an IDE may require the following JVM option to be set: `--add-opens java.base/sun.nio.ch=ALL-UNNAMED`.
