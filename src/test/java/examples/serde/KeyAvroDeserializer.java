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
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KeyAvroDeserializer implements Deserializer<String> {

    KafkaAvroDeserializer inner;

    /**
     * Constructor used by Kafka Streams.
     */
    public KeyAvroDeserializer() {
        inner = new KafkaAvroDeserializer();
    }

    public KeyAvroDeserializer(SchemaRegistryClient client) {
        inner = new KafkaAvroDeserializer(client);
    }

    public KeyAvroDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
        inner = new KafkaAvroDeserializer(client, props);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public String deserialize(String s, byte[] bytes) {
        return (String) inner.deserialize(s, bytes);
    }

    @Override
    public void close() {
        inner.close();
    }
}
