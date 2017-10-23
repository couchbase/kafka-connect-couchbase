# Custom Source Handler Example

You can customize the behavior of the Couchbase Source Connector
by providing your own `SourceHandler` implementation. A custom `SourceHandler`
can skip certain messages, route messages to different topics, or
change the content and format of the messages.

This example can be used as a starting point
for your own custom handler projects.


## Example Scenario

Suppose your Kafka consumers want to receive JSON without schemas (perhaps
they're not yet on the [Avro](https://www.confluent.io/blog/avro-kafka-data/)
bandwagon). You can control the message format by configuring the connector
accordingly:

    value.converter = org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable = false

However, by default the source connector makes no assumptions about the format
of the Couchbase documents. Every document is transported as a byte array,
regardless of whether it's a JSON document. The document content will be
Base64-encoded, and the Kafka messages will look like this:

    {
        "key": "123",
        "content": "eyJncmVldGluZyI6ImhlbGxvIHdvcmxkIn0=",
        ...
    }

Suppose you know the Couchbase documents are always JSON. Let's make it easier
for consumers to work with the document content by including it
directly in the Kafka message, like so:

    {
        "key": "123",
        "content": {
            "greeting" : "hello world"
        },
        ...
    }

To do this you will need to:

1. Write a Java class that extends `SourceHandler` and package it in a JAR.
2. Install it by including the JAR in the same location as
   the kafka-connect-couchbase JAR.
3. Configure the Couchbase source connector to use your
   custom handler and appropriate key/value converters.

## Write the Source Handler

You're in luck! This part is done already. Take a look at the source code in
the `src/main/java` directory.


## Install the Source Handler

Run `mvn package` to build the custom source handler JAR. Copy the JAR from
the `target` directory into the same directory as `kafka-connect-couchabse-<version>.jar`.
(In the Quickstart guide this directory is referred to as `$KAFKA_CONNECT_COUCHBASE_HOME`.)

NOTE: Don't forget to copy the JAR every time you make changes to the Java code.


## Configure the Connector

Make a copy of the `config/quickstart-couchbase-source.properties` file
included with the Couchbase connector. If you've already gone through the
Quickstart guide, the file is configured with an appropriate bucket name
and user credentials (otherwise go ahead and do that now).

Tell the connector to use your custom `SourceHandler` by setting the
`dcp.message.converter.class` property:

    dcp.message.converter.class = com.couchbase.connect.kafka.example.CustomSourceHandler

You're almost done, but there's one more important step. Kafka message keys
and values are just arrays of bytes. The Kafka Connect framework must be told
how to convert the Java objects returned by your `SourceHandler` into byte
arrays. Specify the converters to use by setting the `key.converter` and
`value.converter` properties in your plugin config:

    key.converter = org.apache.kafka.connect.storage.StringConverter
    value.converter = org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable = false

NOTE: If you do not specify converters in the plugin config, default
converters are used. The defaults are specified in the connect *worker*
properties; the initial value may differ between the various Kafka
distributions.


## Try It Out

Start the source connector as described in the Quickstart guide,
but with your customized version of the plugin config properties file.

If all goes well, please enjoy modifying the example `SourceHandler`
to meet the requirements of your own project.


## Troubleshooting

If the connector fails to start, or the messages are not formatted as you expect,
please review this guide and make sure the custom source handler JAR is in the
correct location and the plugin is configured properly.

If you get stuck, help is available on the
[Couchbase Forum](https://forums.couchbase.com/c/Kafka-Connector).
