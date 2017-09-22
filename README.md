# Kafka Connect Couchbase Connector

kafka-connect-couchbase is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data from Couchbase Server into Kafka.

### Grab the Connector

The latest release of the Couchbase connector is available from the
[Couchbase download page](https://www.couchbase.com/downloads) (search the page for "Kafka").

If you prefer to build the connector from source, clone the
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

You'll also need a local installation of Kafka or the Confluent Platform.


## Set Up Kafka

If you already have an installation of Kafka (or the Confluent Platform) and know how to
start the servers, feel free to skip this section.

Still reading? Don't worry, setting up a basic installation is pretty easy.
Download either [Kafka](https://kafka.apache.org/downloads)
or [Confluent](https://www.confluent.io/download/).
For simplicity, this guide assumes you're installing from a ZIP or TAR archive,
so steer clear of the deb/rpm packages for now.

Decompress the Kafka or Confluent archive and move the resulting directory under `~/opt`
(or wherever you like to keep this kind of software).
The rest of this guide refers to the root of the installation directory as `$KAFKA_HOME`
or `$CONFLUENT_HOME`. Be aware that some config files are in different relative locations
depending on whether you're using Kafka or Confluent.

Make sure the Kafka / Confluent command line tools are in your path:

    export PATH=<path-to-kafka-or-confluent>/bin:$PATH


### Start the Kafka / Confluent Servers

If you're using Confluent, start the servers by running these commands,
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
`quickstart-couchbase-source.properties` file:

    name=test-couchbase
    connector.class=com.couchbase.connect.kafka.CouchbaseSourceConnector
    tasks.max=2

    connection.cluster_address=127.0.0.1
    connection.timeout.ms=2000

    # connection.ssl.enabled=true
    # connection.ssl.keystore.location=/tmp/keystore
    # connection.ssl.keystore.password=secret

    connection.bucket=default
    connection.username=default
    connection.password=secret

    topic.name=test-default
    use_snapshots=false

For this exercise, change the value of `connection.bucket` to `travel-sample`
(or whichever bucket you want to stream from). For `connection.username`
and `connection.password`, supply the credentials of a Couchbase user with read access
to the bucket. If you have not yet created such a user, now is a good time to read about
[Creating and Managing Users with the UI](https://developer.couchbase.com/documentation/server/5.0/security/security-rbac-for-admins-and-apps.html).

NOTE: For Couchbase Server versions prior to 5.0, leave the username blank. Set the password property
to the bucket password, or leave it blank if the bucket does not have a password. The sample buckets
do not have passwords.


## Run the Source Connector

Kafka connectors can be run in [standalone or distributed](https://kafka.apache.org/documentation/#connect_running)
mode. For now let's run the connector in standalone mode, using the CLASSPATH environment variable to include the
Couchbase connector JAR in the class path.

For Confluent:

    cd $KAFKA_CONNECT_COUCHBASE_HOME
    env CLASSPATH=./* \
        connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties \
                           config/quickstart-couchbase-source.properties


Or for Kafka:

    cd $KAFKA_CONNECT_COUCHBASE_HOME
    env CLASSPATH=./* \
        connect-standalone.sh $KAFKA_HOME/config/connect-standalone.properties \
                           config/quickstart-couchbase-source.properties


### Alternatively, Run the Connector with Class Loader Isolation

Kafka version 0.11.0 (and Confluent 3.3.0) introduced a mechanism for plugin
class path isolation. To take advantage of this feature, edit the connect worker config file
(the `connect-*.properties` file in the above commands).
Modify the `plugin.path` property to include the parent directory of
`kafka-connect-couchbase-<version>.jar`.

Run the connector using the same commands as above, but omitting the
`env CLASSPATH=./*` prefix. Each Kafka Connect plugin will use a separate class loader,
removing the possibility of dependency conflicts.


## Observe Messages Published by Couchbase Source Connector

For Confluent:

    kafka-avro-console-consumer --bootstrap-server localhost:9092 \
                                --topic test-default --from-beginning

Or for Kafka:

    kafka-console-consumer.sh --bootstrap-server localhost:9092 \
                              --topic test-default --from-beginning


# Experimental Sink Connector

Since release 3.1.0, the library includes an experimental Sink Connector
that reads messages from a Kafka topic and sends them to Couchbase Server.

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


## Configure and Run the Sink Connector

In the `$KAFKA_CONNECT_COUCHBASE_HOME/config` directory there is a file called
`quickstart-couchbase-sink.properties`. Customize this file as described in
 **Configure the Source Connector**, only now the bucket will receive messages
and the user must have *write* access to the bucket.

NOTE: You may wish to
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
the messages will appear in the Couchbase bucket.

If you wish to see how the Couchbase Sink Connector behaves in the absence of message keys, modify the `publishMessage` method in the example source code to set
the message keys to null, then rerun the producer.

Alternatively, if you want the Couchbase document ID to be the airport code,
edit `quickstart-couchbase-sink.properties` and set `couchbase.document.id=/airport`,
restart the sink connector, and run the producer again.


# Contribute

- Questions? : https://forums.couchbase.com/c/Kafka-Connector
- Documentation: http://developer.couchbase.com/documentation/server/current/connectors/kafka-3.1/kafka-intro.html
- Source Code: https://github.com/couchbase/kafka-connect-couchbase
- Issue Tracker: https://issues.couchbase.com/projects/KAFKAC
- Downloads & Release Notes: http://developer.couchbase.com/documentation/server/current/connectors/kafka-3.1/release-notes.html

# License

The project is licensed under the Apache 2 license.
