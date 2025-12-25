# Development

## Repository

The project is available in [the GitHub repository](https://github.com/dyaraev/spark-jms-connector).
Since artifacts are not currently published to any public repository, you will need to build the connector yourself and include the artifacts manually in your project.
For instructions on building the project, see [the corresponding section](development.md#building-the-project) in the documentation.

## Project Structure

The project is organized into multiple modules:

- **common**: Shared configuration and utilities
- **connector-v1**: Connector implementation for Spark DataSourceV1 API
- **connector-v2**: Connector implementation for Spark DataSourceV2 API
- **examples**: Example applications demonstrating usage with the provided ActiveMQ implementation
- **provider-activemq**: Simple implementation of ConnectionFactoryProvider for ActiveMQ

The `connector-v1` module relies on the legacy Spark API. As a result, the logic has to be placed under the `org.apache.spark` package, since that’s the only way to call certain internal, package-private methods.

## Implementing ConnectionFactoryProvider

To use the connector with your JMS provider, you have to implement the `ConnectionFactoryProvider` interface:

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

The project uses a cross-version build.
During the build process, a Spark version can be provided as an argument, allowing the project to be built for that specific Spark version.
By default, Spark 4.1.0 is used, which — like Spark 4.0.x — can only be built for Scala 2.13.
Spark 3.5.x, however, supports builds for either Scala 2.12 or Scala 2.13.
The project can be built using the following sbt command:

```bash 
sbt +assembly
```
Or for specific Spark and Scala versions:

```bash
# for Spark 4.0.1 and Scala 2.13
sbt -Dspark.version=4.0.1 +assembly

# for Spark 3.5.7 and both Scala 2.12 and Scala 2.13
sbt -Dspark.version=3.5.7 +assembly

# for Spark 3.5.7 and Scala 2.12
sbt ++2.12 -Dspark.version=3.5.7 assembly

# for Spark 3.5.7 and Scala 2.13
sbt ++2.13 -Dspark.version=3.5.7 assembly
```

These command will create the following JAR files with specific version instead of placeholders depending on the build configuration:

- `connector-v1/target/scala-{scala}/spark-jms-connector-v1_{scala}_spark-{spark}_{connector}.jar`
- `connector-v2/target/scala-{scala}/spark-jms-connector-v2_{scala}_spark-{spark}_{connector}.jar`
- `provider-activemq/target/scala-{scala}/spark-jms-provider-activemq_{scala}_{spark}_{connector}.jar`

For example:

- `connector-v1/target/scala-2.13/spark-jms-connector-v1_2.13_spark-4.0.1_0.1.1.jar`
- `connector-v2/target/scala-2.13/spark-jms-connector-v2_2.13_spark-4.0.1_0.1.1.jar`
- `provider-activemq/target/scala-2.13/spark-jms-provider-activemq_2.13_4.0.1_0.1.1.jar`

## Contributing

TODO: add contribution guidelines here
