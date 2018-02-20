# Kafka Connect Couchbase Connector

kafka-connect-couchbase is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect)
plugin for transferring data between Couchbase Server and Kafka.

It includes a "source connector" for publishing document change notifications from Couchbase to
a Kafka topic, as well as a "sink connector" that subscribes to one or more Kafka topics and writes the
messages to Couchbase.

This document describes how to configure and run the source and sink connectors.


### Grab the Connector Distribution

The latest release of the Couchbase connector distribution is available from the
[Couchbase download page](https://www.couchbase.com/downloads) (search the page for "Kafka").

If you prefer to build the connectors from source, clone the
[GitHub repository](https://github.com/couchbase/kafka-connect-couchbase)
and run `mvn package` to generate the connector archive
(look for `kafka-connect-couchbase-<version>.zip` in the `target` directory).


# Quickstart

However you obtained the connector archive, go ahead and unzip it now.
The result should be a directory called `kafka-connect-couchbase-<version>`.
The rest of this guide will refer to this directory as `$KAFKA_CONNECT_COUCHBASE_HOME`.

This guide assumes you have already installed Couchbase Server locally
and have loaded the sample bucket called `travel-sample`. (It's fine if you want to use
a different bucket; neither the connector nor this guide depend on the contents of
the documents in the bucket.)

You'll also need a local installation of Apache Kafka or Confluent Platform Kafka.


## Set Up Kafka

If you already have an installation of Kafka and know how to
start the servers, feel free to skip this section.

Still reading? Don't worry, setting up a basic installation is pretty easy.
Download either [Apache Kafka](https://kafka.apache.org/downloads)
or [Confluent Platform Kafka](https://www.confluent.io/download/).
For simplicity, this guide assumes you're installing from a ZIP or TAR archive,
so steer clear of the deb/rpm packages for now.

Decompress the Apache Kafka or Confluent Platform archive and move the resulting directory under `~/opt`
(or wherever you like to keep this kind of software).
The rest of this guide refers to the root of the installation directory as `$KAFKA_HOME`
or `$CONFLUENT_HOME`. Be aware that some config files are in different relative locations
depending on whether you're using Apache Kafka or Confluent Platform Kafka.

Make sure the Kafka command line tools are in your path:

    export PATH=<path-to-apache-kafka-or-confluent>/bin:$PATH


### Start the Kafka Servers

If you're using Confluent Platform Kafka, start the servers by running these commands,
**each in a separate terminal**:

    zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties
    kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties
    schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties

NOTE: Confluent 3.3.0 introduced a CLI tool that lets you start all the servers with a single command.
It also provides a more sophisticated way to manage connectors than the technique described in this guide.
Read about it [here](http://docs.confluent.io/current/connect/quickstart.html).

If you're not using Confluent, the commands are slightly different,
but the idea is the same. Start the servers by running these commands, **each in a separate terminal**:

    zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
    kafka-server-start.sh $KAFKA_HOME/config/server.properties


## Configure the Source Connector

The Couchbase connector distribution includes sample config files. Look inside
`$KAFKA_CONNECT_COUCHBASE_HOME/config` and edit the
`quickstart-couchbase-source.properties` file.

Take a moment to peruse the configuration options specified here. Some are
[standard options available to all connectors](https://kafka.apache.org/documentation/#connect_configuring).
The rest are specific to the Couchbase connector.

For this exercise, change the value of `connection.bucket` to `travel-sample`
(or whichever bucket you want to stream from). For `connection.username`
and `connection.password`, supply the credentials of a Couchbase user who has the "Data DCP Reader"
role for the bucket. If you have not yet created such a user, now is a good time to read about
[Creating and Managing Users with the UI](https://developer.couchbase.com/documentation/server/5.0/security/security-rbac-for-admins-and-apps.html).

NOTE: For Couchbase Server versions prior to 5.0, leave the username blank. Set the password property
to the bucket password, or leave it blank if the bucket does not have a password. The sample buckets
do not have passwords.

You might also want to set `use_snapshots` to `true`, in which case the source connector
will buffer events until it receives a complete snapshot before committing messages to the Kafka topic.
It doesn't matter for this exercise; just be aware the option exists and defaults to `false`.


## Run the Source Connector

Kafka connectors can be run in [standalone or distributed](https://kafka.apache.org/documentation/#connect_running)
mode. For now let's run the connector in standalone mode, using the CLASSPATH environment variable to include the
Couchbase connector JAR in the class path.

For Confluent Platform Kafka:

    cd $KAFKA_CONNECT_COUCHBASE_HOME
    env CLASSPATH=./* \
        connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties \
                           config/quickstart-couchbase-source.properties

Or for Apache Kafka:

    cd $KAFKA_CONNECT_COUCHBASE_HOME
    env CLASSPATH=./* \
        connect-standalone.sh $KAFKA_HOME/config/connect-standalone.properties \
                           config/quickstart-couchbase-source.properties


### Alternatively, Run the Connector with Class Loader Isolation

Apache Kafka version 0.11.0 (and Confluent Platform 3.3.0) introduced a mechanism for plugin
class path isolation. To take advantage of this feature, edit the connect worker config file
(the `connect-*.properties` file in the above commands).
Modify the `plugin.path` property to include the parent directory of
`kafka-connect-couchbase-<version>.jar`.

Run the connector using the same commands as above, but omitting the
`env CLASSPATH=./*` prefix. Each Kafka Connect plugin will use a separate class loader,
removing the possibility of dependency conflicts.


## Observe Messages Published by Couchbase Source Connector

The sample config file tells the source connector to publish to a topic called `test-default`.
Let's use the Kafka command-line tools to spy on the contents of the topic.

For Confluent Platform Kafka:

    kafka-avro-console-consumer --bootstrap-server localhost:9092 \
                                --topic test-default --from-beginning

Or for Apache Kafka:

    kafka-console-consumer.sh --bootstrap-server localhost:9092 \
                              --topic test-default --from-beginning

The expected output is a stream of Couchbase event notification messages,
at least one for each document in the bucket. The messages include document metadata
as well as content. The document content is transferred as a byte array
(encoded as Base64 if the connector is configured to use JSON for message values).

Each message has an `event` field whose value indicates the type of change
represented by the message. The possible values are:

* `mutation`: A change to document content, including creation
              and changes made via subdocument commands.
* `deletion`: Removal or expiration of the document.
* `expiration`: Reserved for document expiration (Couchbase Server
                does not currently send this event type, but may in future versions).

Once the consumer catches up to the current state of the bucket, try
[adding a new document to the bucket via the Couchbase Web Console](https://developer.couchbase.com/documentation/server/current/sdk/webui-cli-access.html).
The consumer will print a notification of type `mutation`. Now delete the document
and watch for an event of type `deletion`.

Perhaps it goes without saying, but all of the offset management and fault tolerance
features of Kafka Connect work with the Couchbase connector.
You can kill and restart the processes and they will pick up where they left off.

The shape of the message payload is controlled by the
`dcp.message.converter.class` property of the connector config.
By default it is set to `com.couchbase.connect.kafka.converter.SchemaConverter`,
which formats each notification into a structure that holds document metadata
and contents. For reference, the Avro schema for this payload format is shown below:

    {
      "type": "record",
      "name": "DcpMessage",
      "namespace": "com.couchbase",
      "fields": [
        {
          "name": "event",
          "type": "string"
        },
        {
          "name": "partition",
          "type": {
            "type": "int",
            "connect.type": "int16"
          }
        },
        {
          "name": "key",
          "type": "string"
        },
        {
          "name": "cas",
          "type": "long"
        },
        {
          "name": "bySeqno",
          "type": "long"
        },
        {
          "name": "revSeqno",
          "type": "long"
        },
        {
          "name": "expiration",
          "type": [
            "null",
            "int"
          ]
        },
        {
          "name": "flags",
          "type": [
            "null",
            "int"
          ]
        },
        {
          "name": "lockTime",
          "type": [
            "null",
            "int"
          ]
        },
        {
          "name": "content",
          "type": [
            "null",
            "bytes"
          ]
        }
      ],
      "connect.name": "com.couchbase.DcpMessage"
    }


# Couchbase Sink Connector

Now let's talk about the sink connector, which reads messages from one or more Kafka topics
and writes them to Couchbase Server.

The sink connector will attempt to convert message values to JSON. If the conversion fails,
the connector will fall back to treating the value as a String BLOB.

If the Kafka key is a primitive type, the connector will use it as the document ID. If the Kafka key
is absent or of complex type (array or struct), the document ID will be generated as
`topic/partition/offset`.

Alternatively, the document ID can come from the body of the Kafka message.
Provide a `couchbase.document.id` property whose value is a JSON Pointer
identifying the document ID node. If you want the connector to remove this node before
persisting the document to Couchbase, provide a `couchbase.remove.document.id`
property with value `true`. If the connector fails to locate the document ID node,
it will fall back to using the Kafka key or `topic/partition/offset` as described above.

If the Kafka message body is null, the sink connector will delete the Couchbase document
whose ID matches the Kafka message key.

## Configure and Run the Sink Connector

In the `$KAFKA_CONNECT_COUCHBASE_HOME/config` directory there is a file called
`quickstart-couchbase-sink.properties`. Customize this file as described in
 **Configure the Source Connector**, only now the bucket will receive messages
and the user must have *write* access to the bucket.

NOTE: Make sure to specify an existing bucket, otherwise the sink connector will fail.
You may wish to
[create a new bucket](https://developer.couchbase.com/documentation/server/current/clustersetup/create-bucket.html)
to receive the messages.

To run the sink connector, use the same command as described in **Run the Source Connector**,
but pass `quickstart-couchbase-sink.properties` as the second argument to `connect-standalone`
instead of `quickstart-couchbase-source.properties`.


## Send Test Messages

Now that the Couchbase Sink Connector is running, let's give it some messages to import:

    cd $KAFKA_CONNECT_COUCHBASE_HOME/examples/json-producer
    mvn compile exec:java

The producer will send some messages and then terminate. If all goes well,
the messages will appear in the Couchbase bucket you specified in the sink connector config.

If you wish to see how the Couchbase Sink Connector behaves in the absence of message keys, modify the `publishMessage` method in the example source code to set
the message keys to null, then rerun the producer.

Alternatively, if you want the Couchbase document ID to be the airport code,
edit `quickstart-couchbase-sink.properties` and set `couchbase.document.id=/airport`,
restart the sink connector, and run the producer again.


# Contribute

- Questions? : https://forums.couchbase.com/c/Kafka-Connector
- Documentation: http://developer.couchbase.com/documentation/server/current/connectors/kafka-3.2/kafka-intro.html
- Source Code: https://github.com/couchbase/kafka-connect-couchbase
- Issue Tracker: https://issues.couchbase.com/projects/KAFKAC
- Downloads & Release Notes: http://developer.couchbase.com/documentation/server/current/connectors/kafka-3.2/release-notes.html

# License

The project is licensed under the Apache 2 license.
