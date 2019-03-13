# Custom Extensions Example

This Maven project is a template you can use for developing your own
custom extension classes for Kafka Connect Couchbase.

You can customize the behavior of the Couchbase Source Connector
by providing your own `SourceHandler` implementation. A custom `SourceHandler`
can skip certain messages, route messages to different topics, or
change the content and format of the messages.

The Sink Connector can modify incoming documents by applying
[Single Message Transforms](https://kafka.apache.org/documentation/#connect_transforms).
This project includes an example of a custom transform you can modify to suit your needs
in case Kafka's built-in transforms are insufficient. 

## Build and Install the Example Extensions

Run `mvn package` to build the custom extensions JAR. Copy the JAR from
the `target` directory into the same directory as `kafka-connect-couchabse-<version>.jar`.
(In the Quickstart guide this directory is referred to as `$KAFKA_CONNECT_COUCHBASE_HOME`.)

NOTE: Don't forget to copy the JAR every time you change the Java code.

The source code of each extension includes Javadoc with instructions
for configuring the connector to use the extension.

If you get stuck, help is available on the
[Couchbase Forum](https://forums.couchbase.com/c/Kafka-Connector).
