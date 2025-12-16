# Development

## Project Structure

The project is organized into multiple modules:

- **common**: Shared configuration and utilities
- **connector-v1**: Connector implementation for Spark DataSourceV1 API
- **connector-v2**: Connector implementation for Spark DataSourceV2 API
- **examples**: Example applications demonstrating usage with the provided ActiveMQ implementation
- **provider-activemq**: Simple implementation of ConnectionFactoryProvider for ActiveMQ

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

## Building the Project

The project can be built using the following sbt command:

```bash 
sbt assembly
```

This will create JAR files in the target directories:

- `connector-v1/target/scala-2.12/spark-jms-connector-v1-<version>.jar`
- `connector-v2/target/scala-2.12/spark-jms-connector-v2-<version>.jar`
- `provider-activemq/target/scala-2.12/spark-jms-provider-activemq-<version>.jar`

## Contributing

TODO: add contribution guidelines here
