# Getting Started

## Reading from JMS Queue

By default, the JMS source provides `At-Least-Once` delivery guarantee. `Exactly-Once` delivery can be achieved by using specific output formats, for instance, Delta Lake.

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
  .option("jms.connection.broker.name", "my-broker")
  .option("jms.connection.queueName", "myQueue")
  .option("jms.messageFormat", "text")
  .option("jms.receiveTimeoutMs", "1000")
  .option("jms.commitIntervalMs", "10000")
  .option("jms.numPartitions", "8")
  .load()
```

## Writing to JMS Queue

The connector does not guarantee `Exactly-Once` delivery for sink operations.

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
  .option("jms.connection.broker.name", "my-broker")
  .option("jms.connection.queueName", "myQueue")
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

The sink component of the connector includes retry logic that performs up to three attempts with exponential backoff, helping to handle temporary connection issues.

## Running Examples

The project includes examples for working with ActiveMQ. To run each example, you have to execute two commands:

```bash
# Run an ActiveMQ broker, a test message generator and an example Spark job with the JMS source
sbt "examples/runMain io.github.dyaraev.spark.connector.jms.example.ExampleApp receiver-job --workingDirectory /tmp/spark-receiver-job"
```

```bash
# Run an ActiveMQ broker, a test file generator, an example Spark job with the JMS sink and a test message reader
sbt "examples/runMain io.github.dyaraev.spark.connector.jms.example.ExampleApp sender-job --workingDirectory /tmp/spark-sender-job"
```

You can provide Spark and Scala versions to the command in the same way as for [building the artifacts](development.md#building-the-project).
You can also play with the example applications by changing the configuration options.
All available options are listed when running commands with the `--help` flag.

Executing examples in an IDE may require the following JVM option to be set: `--add-opens java.base/sun.nio.ch=ALL-UNNAMED`.
