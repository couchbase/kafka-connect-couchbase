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

package examples;

import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import examples.serde.KeyAvroSerde;
import examples.serde.ValueAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class KafkaStreamsDemo {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws InterruptedException, SQLException {
        /**
         * The example assumes the following SQL schema
         *
         *    DROP DATABASE IF EXISTS beer_sample_sql;
         *    CREATE DATABASE beer_sample_sql CHARACTER SET utf8 COLLATE utf8_general_ci;
         *    USE beer_sample_sql;
         *
         *    CREATE TABLE breweries (
         *       id VARCHAR(256) NOT NULL,
         *       name VARCHAR(256),
         *       description TEXT,
         *       country VARCHAR(256),
         *       city VARCHAR(256),
         *       state VARCHAR(256),
         *       phone VARCHAR(40),
         *       updated_at DATETIME,
         *       PRIMARY KEY (id)
         *    );
         *
         *
         *    CREATE TABLE beers (
         *       id VARCHAR(256) NOT NULL,
         *       brewery_id VARCHAR(256) NOT NULL,
         *       name VARCHAR(256),
         *       category VARCHAR(256),
         *       style VARCHAR(256),
         *       description TEXT,
         *       abv DECIMAL(10,2),
         *       ibu DECIMAL(10,2),
         *       updated_at DATETIME,
         *       PRIMARY KEY (id)
         *    );
         */
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Failed to load MySQL JDBC driver");
        }
        Connection connection = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/beer_sample_sql", "root", "secret");
        final PreparedStatement insertBrewery = connection.prepareStatement(
                "INSERT INTO breweries (id, name, description, country, city, state, phone, updated_at)" +
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)" +
                        " ON DUPLICATE KEY UPDATE" +
                        " name=VALUES(name), description=VALUES(description), country=VALUES(country)," +
                        " country=VALUES(country), city=VALUES(city), state=VALUES(state)," +
                        " phone=VALUES(phone), updated_at=VALUES(updated_at)");
        final PreparedStatement insertBeer = connection.prepareStatement(
                "INSERT INTO beers (id, brewery_id, name, description, category, style, abv, ibu, updated_at)" +
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                        " ON DUPLICATE KEY UPDATE" +
                        " brewery_id=VALUES(brewery_id), name=VALUES(name), description=VALUES(description)," +
                        " category=VALUES(category), style=VALUES(style), abv=VALUES(abv)," +
                        " ibu=VALUES(ibu), updated_at=VALUES(updated_at)");

        String schemaRegistryUrl = "http://localhost:8081";

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, KeyAvroSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, ValueAvroSerde.class);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, GenericRecord> source = builder
                .stream("streaming-topic-beer-sample");

        KStream<String, JsonNode>[] documents = source
                .mapValues(new ValueMapper<GenericRecord, JsonNode>() {
                    @Override
                    public JsonNode apply(GenericRecord value) {
                        ByteBuffer buf = (ByteBuffer) value.get("content");
                        try {
                            JsonNode doc = MAPPER.readTree(buf.array());
                            return doc;
                        } catch (IOException e) {
                            return null;
                        }
                    }
                })
                .branch(
                        new Predicate<String, JsonNode>() {
                            @Override
                            public boolean test(String key, JsonNode value) {
                                return "beer".equals(value.get("type").asText()) &&
                                        value.has("brewery_id") &&
                                        value.has("name") &&
                                        value.has("description") &&
                                        value.has("category") &&
                                        value.has("style") &&
                                        value.has("abv") &&
                                        value.has("ibu") &&
                                        value.has("updated");
                            }
                        },
                        new Predicate<String, JsonNode>() {
                            @Override
                            public boolean test(String key, JsonNode value) {
                                return "brewery".equals(value.get("type").asText()) &&
                                        value.has("name") &&
                                        value.has("description") &&
                                        value.has("country") &&
                                        value.has("city") &&
                                        value.has("state") &&
                                        value.has("phone") &&
                                        value.has("updated");
                            }
                        }
                );
        documents[0].foreach(new ForeachAction<String, JsonNode>() {
            @Override
            public void apply(String key, JsonNode value) {
                try {
                    insertBeer.setString(1, key);
                    insertBeer.setString(2, value.get("brewery_id").asText());
                    insertBeer.setString(3, value.get("name").asText());
                    insertBeer.setString(4, value.get("description").asText());
                    insertBeer.setString(5, value.get("category").asText());
                    insertBeer.setString(6, value.get("style").asText());
                    insertBeer.setBigDecimal(7, new BigDecimal(value.get("abv").asText()));
                    insertBeer.setBigDecimal(8, new BigDecimal(value.get("ibu").asText()));
                    insertBeer.setDate(9, new Date(DATE_FORMAT.parse(value.get("updated").asText()).getTime()));
                    insertBeer.execute();
                } catch (SQLException e) {
                    System.err.println("Failed to insert record: " + key + ". " + e);
                } catch (ParseException e) {
                    System.err.println("Failed to insert record: " + key + ". " + e);
                }
            }
        });
        documents[1].foreach(new ForeachAction<String, JsonNode>() {
            @Override
            public void apply(String key, JsonNode value) {
                try {
                    insertBrewery.setString(1, key);
                    insertBrewery.setString(2, value.get("name").asText());
                    insertBrewery.setString(3, value.get("description").asText());
                    insertBrewery.setString(4, value.get("country").asText());
                    insertBrewery.setString(5, value.get("city").asText());
                    insertBrewery.setString(6, value.get("state").asText());
                    insertBrewery.setString(7, value.get("phone").asText());
                    insertBrewery.setDate(8, new Date(DATE_FORMAT.parse(value.get("updated").asText()).getTime()));
                    insertBrewery.execute();
                } catch (SQLException e) {
                    System.err.println("Failed to insert record: " + key + ". " + e);
                } catch (ParseException e) {
                    System.err.println("Failed to insert record: " + key + ". " + e);
                }
            }
        });

        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));
    }
}