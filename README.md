# Kafka Connect Couchbase Connector

kafka-connect-couchbase is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data from Couchbase Server database into Kafka.

## Quickstart

Start by setting up Couchbase Server and loading sample bucket called `travel-sample`.

To build a development version you'll need a recent version of Kafka. You can build
kafka-connect-couchbase with Maven using the standard lifecycle phases. Build the connector jar:

    $ mvn package

After this command, you've got package directory which has the same structure as other Confluent
connectors and ready to be installed into the system. Assuming you have Confluent platform
installed as in this guide: http://docs.confluent.io/3.0.0/installation.html, you can copy
the package to the system location:

    $ sudo cp -a target/kafka-connect-couchbase-3.0.0-package/share /usr/

Now we create a configuration file that will load data from this database. This file is included
with the connector in `/etc/kafka-connect-couchbase/quickstart-couchbase.properties` and contains
the following settings:

    name=test-couchbase
    connector.class=com.couchbase.connect.kafka.CouchbaseSourceConnector
    tasks.max=1
    connection.cluster_address=127.0.0.1
    connection.bucket=travel-sample
    connection.timeout.ms=2000
    # connection.ssl.enabled=true
    # connection.ssl.keystore.location=/tmp/keystore
    # connection.ssl.keystore.password=secret
    topic.name=test-couchbase
    use_snapshots=false

Start Zookeeper. Since this is a long-running service, you should run it in its own terminal.

    $ sudo zookeeper-server-start /etc/kafka/zookeeper.properties

Start Kafka, also in its own terminal.

    $ sudo kafka-server-start /etc/kafka/server.properties

Start the Schema Registry, also in its own terminal.

    $ sudo schema-registry-start /etc/schema-registry/schema-registry.properties

Create new settings for Connect which force Avro converter (this assumes that Kafka and 
the Schema Registry are running locally on the default ports
`/etc/kafka-connect-couchbase/connect-standalone.properties`:

    bootstrap.servers=localhost:9092
    key.converter=io.confluent.connect.avro.AvroConverter
    key.converter.schema.registry.url=http://localhost:8081
    value.converter=io.confluent.connect.avro.AvroConverter
    value.converter.schema.registry.url=http://localhost:8081
    key.converter.schemas.enable=true
    value.converter.schemas.enable=true
    internal.key.converter=io.confluent.connect.avro.AvroConverter
    internal.key.converter.schema.registry.url=http://localhost:8081
    internal.value.converter=io.confluent.connect.avro.AvroConverter
    internal.value.converter.schema.registry.url=http://localhost:8081
    internal.key.converter.schemas.enable=false
    internal.value.converter.schemas.enable=false
    offset.storage.file.filename=/tmp/connect.offsets
    offset.flush.interval.ms=10000

Now, run the connector in a standalone Kafka Connect worker in another terminal:

    $ sudo connect-standalone /etc/kafka-connect-couchbase/connect-standalone.properties \
                              /etc/kafka-connect-couchbase/quickstart-couchbase.properties

To observe replicated events from the cluster, run CLI kafka consumer:

    $ kafka-avro-console-consumer --new-consumer --bootstrap-server localhost:9092 \
                                  --topic test-couchbase --from-beginning

# Contribute

- Documentation: http://developer.couchbase.com/documentation/server/4.5/connectors/kafka-3.0/kafka-intro.html
- Source Code: https://github.com/couchbase/kafka-connect-couchbase
- Issue Tracker: https://issues.couchbase.com/projects/KAFKAC
- Downloads & Release Notes: http://developer.couchbase.com/documentation/server/4.5/connectors/kafka-3.0/release-notes.html

# License

The project is licensed under the Apache 2 license.
