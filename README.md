# Spark JMS Connector

A JMS connector for Apache Spark that provides functionality for reading from and writing to JMS queues using Spark Structured Streaming.
This is a fully functional implementation of Spark JMS connector, but it was created for educational purposes and has some [limitations](#limitations-and-considerations).
The main purpose of this project is to provide a working example of how to implement a JMS connector for Apache Spark.

_Please read [the disclaimer](#disclaimer) below before using this connector._

## Table of Contents

- [Features](#features)
- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Usage](#usage)
    - [Reading from JMS Queue](#reading-from-jms-queue)
    - [Writing to JMS Queue](#writing-to-jms-queue)
- [Implementing ConnectionFactoryProvider](#implementing-connectionfactoryprovider)
- [Configuration Options](#configuration-options)
    - [Connection Options](#connection-options)
    - [Source Options](#source-options)
    - [Sink Options](#sink-options)
- [Building](#building-the-project)
- [Running Examples](#running-examples)
- [Limitations and Considerations](#limitations-and-considerations)
- [Contributing](#contributing)
- [Disclaimer](#disclaimer)

## Features

- **V1 and V2 APIs Support**: Includes both Spark DataSourceV1 and DataSourceV2 implementations.
- **Streaming Source Support**: Reads from JMS queues as Spark streaming sources.
- **Streaming Sink Support**: Writes Spark streaming data to JMS destinations.
- **Configurable Message Format**: Supports text and binary formats.
- **At-Least-Once Delivery for Source**: Messages are acknowledged after being successfully stored using a write-ahead log.
- **Provider-Agnostic**: Works with any JMS-compliant messaging system (ActiveMQ, IBM MQ, etc.) but requires a corresponding implementation of `ConnectionFactoryProvider`.

## Project Structure

The project is organized into multiple modules:

- **common**: Shared configuration and utilities
- **connector-v1**: Connector implementation for Spark DataSourceV1 API
- **connector-v2**: Connector implementation for Spark DataSourceV2 API
- **examples**: Example applications demonstrating usage with the provided ActiveMQ implementation
- **provider-activemq**: Simple implementation of ConnectionFactoryProvider for ActiveMQ

## Requirements

- Java 17
- Scala 2.12.x
- Apache Spark 3.5.x
- Jakarta JMS API 3.1.0

## Usage

### Reading from JMS Queue

By default, the JMS source provides `At-Least-Once` delivery guarantee. `Exactly-Once` delivery can be achieved by using specific output formats, for instance, Delta Lake.

The resulting DataFrame schema returned by the source consists of the following fields:
* `StructField("id", StringType)` - Message ID
* `StructField("sent", LongType)` - Timestamp when the message was sent
* `StructField("received", LongType)` - Timestamp when the message was received
* `StructField("value", <type>)` - Message content

The `value` field type depends on the `jms.source.messageFormat` option:
- When set to `text`: expects `TextMessage` and the value field is of type `StringType` (text content)
- When set to `binary`: expects `BytesMessage` and the value field is of type `BinaryType` (byte array)

This is an example of how to use the connector to read data from a JMS queue:

```scala
val df = spark.readStream
  .format("jms-v2") // or "jms-v1" depending on the used implementation
  .option("jms.connection.queueName", "myQueue")
  .option("jms.connection.factoryProvider", "com.example.MyConnectionFactoryProvider")
  // broker specific options for the provided connection factory implementation
  .option("jms.source.messageFormat", "text")
  .option("jms.source.receiveTimeoutMs", "1000")
  .option("jms.source.commitIntervalMs", "10000")
  .option("jms.source.numPartitions", "8")
  .load()
```

### Writing to JMS Queue

The connector does not guarantee `Exactly-Once` delivery for sink operations.

The input DataFrame must contain a field named `value` that holds the message content to be sent. The type of the field should be either `StringType` or `BinaryType`. The connector automatically handles type conversion based on the `jms.sink.messageFormat` option:

| Field Type   | Message Format | JMS Message Type | Conversion Behavior                    |
|--------------|----------------|------------------|----------------------------------------|
| `StringType` | `text`         | `TextMessage`    | Sent as-is                             |
| `BinaryType` | `text`         | `TextMessage`    | `Array[Byte]` is converted to `String` |
| `BinaryType` | `binary`       | `BytesMessage`   | Sent as-is                             |
| `StringType` | `binary`       | `BytesMessage`   | `String` is converted to `Array[Byte]` |


This is an example that demonstrates how to use the connector to write data to a JMS queue:

```scala 
df.writeStream
  .format("jms-v2") // or "jms-v1" depending on the used implementation
  .option("jms.connection.queueName", "myQueue")
  .option("jms.connection.factoryProvider", "com.example.MyConnectionFactoryProvider")
  // broker specific options for the provided connection factory implementation
  .option("jms.sink.messageFormat", "text")
  .option("checkpointLocation", "/tmp/checkpoint")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()
  .awaitTermination()
```

It's important to note that the sink creates one JMS connection per partition when sending messages because each partition processes its data independently.
The implementation may not be optimal, as it does not use connection pooling when connecting to JMS brokers.

```scala
// Limit to 1 concurrent JMS connection
df.repartition(1)
  .writeStream
  .format("jms-v2")
  // other options
  .start()
```

The sink component of the connector includes retry logic that performs up to three attempts with exponential backoff, helping to handle temporary connection issues.

## Implementing ConnectionFactoryProvider

To use with your JMS provider, implement the `ConnectionFactoryProvider` interface:

```scala
import io.github.dyaraev.spark.connector.jms.common.ConnectionFactoryProvider 
import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap
import jakarta.jms.ConnectionFactory

class MyConnectionFactoryProvider extends ConnectionFactoryProvider {
  
  override val brokerName: String = "my-broker"

  override def getConnectionFactory(options: CaseInsensitiveConfigMap): ConnectionFactory = {
    // Return your JMS provider's ConnectionFactory
    // You can access broker-specific options from the `options` parameter
  }
}
```

As an example, the `provider-activemq` module offers a simple implementation of `ConnectionFactoryProvider` for ActiveMQ.

## Configuration Options

The available broker configuration options depend on the implementation of `ConnectionFactoryProvider` being used. 
Each broker provider defines its own set of options. 
For example, when using `ActiveMqConnectionFactoryProvider` from the `provider-activemq` module, you can configure the broker URL:

```scala
df.writeStream
  .format("jms-v2")
  .option("jms.connection.broker.url", "tcp://localhost:61616")
  // other options
  .start()
```

### Connection Options

The following options can be provided to both DataStreamReader and DataStreamWriter:

| Option                           | Required  | Description                                                                 |
|----------------------------------|-----------|-----------------------------------------------------------------------------|
| `jms.connection.queueName`       | Yes       | Name of the JMS queue to read messages from                                 |
| `jms.connection.factoryProvider` | Yes       | Fully qualified class name of the ConnectionFactory provider implementation |
| `jms.connection.username`        | No        | Username for JMS connection authentication                                  |
| `jms.connection.password`        | No        | Password for JMS connection authentication                                  |
| `jms.connection.messageSelector` | No        | JMS message selector for filtering messages                                 |

### Source Options

The following options can be provided to both DataStreamReader:

| Option                        | Required | Description                                                                                                              |
|-------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------|
| `jms.source.messageFormat`    | Yes      | Message format that depends on the Message type (TextMessage or BytesMessage): `text` or `binary`                        |
| `jms.source.commitIntervalMs` | Yes      | Interval in milliseconds to use for storing received messages in the write-ahead log                                     |
| `jms.source.receiveTimeoutMs` | No       | Timeout in milliseconds for using when Consumer.receive(...) is called; if omitted Consumer.receiveNoWait() will be used |
| `jms.source.bufferSize`       | No       | Size of the internal buffer to store messages before they are written into the write-ahead log (defaults to 5000)        |
| `jms.source.numOffsetsToKeep` | No       | Number of offsets in the Spark metadata directory to retain for tracking (defaults to 100)                               |
| `jms.source.numPartitions`    | No       | Number of partitions to split incoming data into (uses value of `spark.sparkContext.defaultParallelism` if not provided) |

### Sink Options

The following options can be provided to both DataStreamWriter:

| Option                       | Required | Description                                                                                       |
|------------------------------|----------|---------------------------------------------------------------------------------------------------|
| `jms.sink.messageFormat`     | Yes      | Message format that depends on the Message type (TextMessage or BytesMessage): `text` or `binary` |
| `jms.sink.throttlingDelayMs` | No       | Delay in milliseconds for throttling message writes (by default no throttling is applied)         |

## Building the Project

The project can be built using the following sbt command:

```bash 
sbt assembly
```

This will create JAR files in the target directories:
- `connector-v1/target/scala-2.12/spark-jms-connector-v1-<version>.jar`
- `connector-v2/target/scala-2.12/spark-jms-connector-v2-<version>.jar`

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

You can play with the example applications by changing the configuration options.
All available options are listed when running commands with the `--help` flag.

Executing examples in an IDE may require the following JVM option to be set: `--add-opens java.base/sun.nio.ch=ALL-UNNAMED`.

## Limitations and Considerations

The connector is still in development and may contain bugs and limitations. 
Since it's mostly a proof-of-concept, it's not recommended for production use.
Some of the limitations include:
- JMS connections are not pooled.
- No full support of the JMS 2.0 specification.
- Receiving messages from a queue is done in a driver in a single threaded manner, so it may affect performance in distributed environments.
- Sending messages is done in executors, so every executor task creates its own connection.
- The number of connections used by the sink component depends on the number of partitions.
- Connections in the sink component are created for each batch and not reused.
- The connector uses a fail-fast strategy, so no proper retry logic for failed writes is implemented.
- Messages are written to the write-ahead log using Java serialization, which may be non-optimal, especially for large messages.

## Contributing

TODO: add contribution guidelines here

## Disclaimer

**THIS SOFTWARE IS PROVIDED "AS IS" FOR EDUCATIONAL AND EXPERIMENTAL PURPOSES.**

This connector should be used with caution in production environments. The author(s) and contributors make no warranties, express or implied, about the completeness, reliability, or suitability of this software for any particular purpose.

**The author(s) accept NO RESPONSIBILITY for any consequences, damages, or losses arising from the use of this software, including but not limited to:**
- Data loss or corruption
- System failures or downtime
- Performance issues
- Security vulnerabilities
- Financial losses

**Before deploying to production pipelines:**
- Thoroughly test it in a development/staging environment
- Implement proper error handling and monitoring
- Ensure you have adequate backup and recovery procedures
- Review and understand the code thoroughly
- Consider your specific use case and requirements

Use at your own risk.
