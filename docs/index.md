# Spark JMS Connector

Spark JMS connector provides functionality for reading from and writing to JMS queues using Spark Structured Streaming.
To ensure at-least-once delivery for streaming sources, the connector uses a write-ahead log to keep messages that haven’t yet been successfully written to the destination.
The project includes two versions of the connector: one built on Spark DataSourceV1 and another on DataSourceV2, with both supporting streaming sources and sinks.
The implementations support similar configuration options and features.
They use a provider-agnostic approach to connect to different JMS messaging systems.
Both implementations are fully functional, but it was created for educational purposes and has some [limitations](#limitations-and-considerations).
The main purpose of this project is to provide a working example of how to implement a JMS connector for Apache Spark.

_Please read [the disclaimer](#disclaimer) below before using this connector._

## Features

- **V1 and V2 APIs Support**: Includes both Spark DataSourceV1 and DataSourceV2 implementations.
- **Streaming Source Support**: Reads from JMS queues as Spark streaming sources.
- **Streaming Sink Support**: Writes Spark streaming data to JMS destinations.
- **Configurable Message Format**: Supports text (`jakarta.jms.TextMessage`) and binary (`jakarta.jms.BytesMessage`) formats.
- **At-Least-Once Delivery for Source**: Messages are acknowledged after being successfully stored using a write-ahead log.
- **Provider-Agnostic**: Works with any JMS-compliant messaging system (ActiveMQ, IBM MQ, etc.) but requires a corresponding implementation of `ConnectionFactoryProvider`.

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

## License

This project is released under the [MIT License](../LICENSE), which allows you to freely use, modify, and distribute the code with minimal restrictions. 
By using this project, you agree to the terms outlined in the license.

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
