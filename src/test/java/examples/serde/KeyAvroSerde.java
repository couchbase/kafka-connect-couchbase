/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package examples.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.Map;

public class KeyAvroSerde implements Serde<String> {

    private final Serde<String> inner;

    /**
     * Constructor used by Kafka Streams.
     */
    public KeyAvroSerde() {
        inner = Serdes.serdeFrom(new KeyAvroSerializer(), new KeyAvroDeserializer());
    }

    public KeyAvroSerde(SchemaRegistryClient client) {
        this(client, Collections.<String, Object>emptyMap());
    }

    public KeyAvroSerde(SchemaRegistryClient client, Map<String, ?> props) {
        inner = Serdes.serdeFrom(new KeyAvroSerializer(client), new KeyAvroDeserializer(client, props));
    }

    @Override
    public Serializer<String> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<String> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }
}
