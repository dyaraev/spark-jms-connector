# Spark JMS Connector

A JMS connector for Apache Spark that provides functionality for reading from and writing to JMS queues using Spark Structured Streaming.
To ensure at-least-once delivery for streaming sources, the connector uses a write-ahead log to keep messages that haven’t yet been successfully written to the destination.
This is a fully functional implementation of Spark JMS connector, but it was created for educational purposes and has some [limitations](https://dyaraev.github.io/spark-jms-connector/#limitations-and-considerations).
The main purpose of this project is to provide a working example of how to implement a JMS connector for Apache Spark.

_Please read [the disclaimer](#disclaimer) below before using this connector._

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Usage Examples](#usage-examples)
- [Disclaimer](#disclaimer)

## Features

- **V1 and V2 APIs Support**: Includes both Spark DataSource V1 and V2 implementations.
- **Streaming Source Support**: Reads from JMS queues as Spark streaming sources.
- **Streaming Sink Support**: Writes Spark streaming data to JMS destinations.
- **Configurable Message Format**: Supports text and binary formats.
- **At-Least-Once Delivery for Source**: Messages are acknowledged after being successfully stored using a write-ahead log.
- **Provider-Agnostic**: Works with any JMS-compliant messaging system (ActiveMQ, IBM MQ, etc.) but requires a corresponding implementation of `ConnectionFactoryProvider`.

## Requirements

- Java 17
- Scala 2.12.x / 2.13.x
- Apache Spark 3.5.x / 4.0.x / 4.1.x
- Jakarta JMS API 3.1.0

_See more about the requirements in the [documentation](https://dyaraev.github.io/spark-jms-connector/getting-started/#requirements)._

## Usage Examples

#### Reading from JMS Queue

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

#### Writing to JMS Queue

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

_More information about the connector and its configuration can be found in the [documentation](https://dyaraev.github.io/spark-jms-connector/)._

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
