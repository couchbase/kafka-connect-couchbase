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
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KeyAvroSerializer implements Serializer<String> {

    KafkaAvroSerializer inner;

    /**
     * Constructor used by Kafka Streams.
     */
    public KeyAvroSerializer() {
        inner = new KafkaAvroSerializer();
    }

    public KeyAvroSerializer(SchemaRegistryClient client) {
        inner = new KafkaAvroSerializer(client);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, String key) {
        return inner.serialize(topic, key);
    }

    @Override
    public void close() {
        inner.close();
    }
}
