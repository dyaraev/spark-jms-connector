# Configuration

The available broker configuration options depend on the implementation of `ConnectionFactoryProvider` being used.
Each broker provider defines its own set of options.
For example, when using `ActiveMqConnectionFactoryProvider` from the `provider-activemq` module, you can configure the broker URL:

```scala
import io.github.dyaraev.spark.connector.jms.provider.activemq.ActiveMqConnectionFactoryProvider

df.writeStream
  .format("jms-v2")
  .option("jms.connection.broker.url", "tcp://localhost:61616")
  .option("jms.connection.factoryProvider", classOf[ActiveMqConnectionFactoryProvider].getName)
  // other options
  .start()
```

Depending on the connector implementation, you must use the corresponding format value for both `DataStreamReader` and `DataStreamWriter`, such as `jms-v1` or `jms-v2`.
It is recommended to use the DataSource V2 implementation by specifying `jms-v2` as the format value.

## Connection Options

The following options can be provided to both `DataStreamReader` and `DataStreamWriter`:

/// html | div.conf-table

| Option                           | Required  | Description                                                                   |
|----------------------------------|-----------|-------------------------------------------------------------------------------|
| `jms.connection.queueName`       | Yes       | Name of the JMS queue to read messages from                                   |
| `jms.connection.factoryProvider` | Yes       | Fully qualified class name of the `ConnectionFactory` provider implementation |
| `jms.connection.username`        | No        | Username for JMS connection authentication                                    |
| `jms.connection.password`        | No        | Password for JMS connection authentication                                    |
| `jms.connection.messageSelector` | No        | JMS message selector for filtering messages                                   |

///

## Source Options

The following options can be provided to `DataStreamReader`:

/// html | div.conf-table

| Option                        | Required | Description                                                                                                                  |
|-------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------|
| `jms.source.messageFormat`    | Yes      | Message format that depends on the `Message` implementation (`TextMessage` or `BytesMessage`): `text` or `binary`            |
| `jms.source.commitIntervalMs` | Yes      | Interval in milliseconds to use for storing received messages in the write-ahead log                                         |
| `jms.source.receiveTimeoutMs` | No       | Timeout in milliseconds for using when `Consumer.receive(...)` is called; if omitted `Consumer.receiveNoWait()` will be used |
| `jms.source.bufferSize`       | No       | Size of the internal buffer to store messages before they are written into the write-ahead log (defaults to 5000)            |
| `jms.source.numOffsetsToKeep` | No       | Number of offsets in the Spark metadata directory to retain for tracking (defaults to 100)                                   |
| `jms.source.numPartitions`    | No       | Number of partitions to split incoming data into (uses value of `spark.sparkContext.defaultParallelism` if not provided)     |

///

## Sink Options

The following options can be provided to `DataStreamWriter`:

/// html | div.conf-table

| Option                       | Required | Description                                                                                                       |
|------------------------------|----------|-------------------------------------------------------------------------------------------------------------------|
| `jms.sink.messageFormat`     | Yes      | Message format that depends on the `Message` implementation (`TextMessage` or `BytesMessage`): `text` or `binary` |
| `jms.sink.throttlingDelayMs` | No       | Delay in milliseconds for throttling message writes (by default no throttling is applied)                         |

///