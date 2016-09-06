# Kafka Connect Couchbase Connector

kafka-connect-couchbase is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data from Couchbase Server database into Kafka.

## Quickstart

Start by setting up Couchbase Server, and loading sample bucket called `travel-sample`.

To build a development version you'll need a recent version of Kafka. You can build
kafka-connect-couchbase with Maven using the standard lifecycle phases. Build the connector jar:

    $ mvn package

After this command, you've got package directory which has the same structure as other Confluent
connectors and ready to be installed into the system. Assuming you have Confluent platform
installed as in this guide: http://docs.confluent.io/3.0.0/installation.html, you can copy
the package to the system location:

    $ sudo cp -a target/kafka-connect-couchbase-1.0.0-package/share /usr/

Now we create a configuration file that will load data from this database. This file is included
with the connector in `/etc/kafka-connect-couchbase/quickstart-couchbase.properties` and contains
the following settings:

    name=test-couchbase
    connector.class=com.couchbase.connect.kafka.CouchbaseSourceConnector
    tasks.max=1
    connection.cluster_address=127.0.0.1
    connection.bucket=travel-sample
    connection.timeout_ms=2000
    topic.name=test-couchbase

Start Zookeeper. Since this is a long-running service, you should run it in its own terminal.

    $ sudo zookeeper-server-start /etc/kafka/zookeeper.properties

Start Kafka, also in its own terminal.

    $ sudo kafka-server-start /etc/kafka/server.properties

Start the Schema Registry, also in its own terminal.

    $ sudo schema-registry-start /etc/schema-registry/schema-registry.properties

Now, run the connector in a standalone Kafka Connect worker in another terminal (this assumes
Avro settings and that Kafka and the Schema Registry are running locally on the default ports):

    $ sudo connect-standalone /etc/schema-registry/connect-avro-standalone.properties \
                              /etc/kafka-connect-couchbase/quickstart-couchbase.properties

To observe replicated events from the cluster, run CLI kafka consumer:

    $ kafka-avro-console-consumer --new-consumer --bootstrap-server localhost:9092 \
                                  --topic test-couchbase --from-beginning

# Contribute

- Source Code: https://github.com/couchbaselabs/kafka-connect-couchbase
- Issue Tracker: https://issues.couchbase.com/projects/KAFKAC

# License

The project is licensed under the Apache 2 license.