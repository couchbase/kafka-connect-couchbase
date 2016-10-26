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

package com.couchbase.connect.kafka.tools;

import com.couchbase.connect.kafka.util.Schemas;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class SchemaManager {
    public static void main(String[] args) throws IOException, RestClientException {
        String schemaRegistryUrl = "http://localhost:8081";

        OptionParser optionParser = new OptionParser();
        optionParser.formatHelpWith(new BuiltinHelpFormatter(120, 2));
        optionParser.accepts("dump-default-schema", "Print default schema to standard output.");
        optionParser.accepts("schema-registry-url", "URL of SchemaRegistry service (" + schemaRegistryUrl + ")").withRequiredArg();
        optionParser.accepts("schema-subject", "Subject where to register schema").withRequiredArg();
        optionParser.accepts("schema-path", "Path to new schema").withRequiredArg();

        if (args.length == 0) {
            try {
                System.out.println("Work with schema definitions for Couchbase connector");
                optionParser.printHelpOn(System.out);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.exit(0);
        }
        OptionSet options = optionParser.parse(args);

        AvroData avroData = new AvroData(10);
        Schema defaultSchema = avroData.fromConnectSchema(Schemas.VALUE_DEFAULT_SCHEMA);

        if (options.has("dump-default-schema")) {
            System.out.println(defaultSchema.toString(true));
            System.exit(0);
        } else {
            String schemaSubject = null, schemaPath = null;
            if (options.hasArgument("schema-registry-url")) {
                schemaRegistryUrl = (String) options.valueOf("schema-registry-url");
            }
            if (options.hasArgument("schema-path")) {
                schemaPath = (String) options.valueOf("schema-path");
            } else {
                System.err.println("--schema-path <path> is required");
                optionParser.printHelpOn(System.err);
                System.exit(1);
            }
            if (options.hasArgument("schema-subject")) {
                schemaSubject = (String) options.valueOf("schema-subject");
            } else {
                System.err.println("--schema-subject <subject> is required");
                optionParser.printHelpOn(System.err);
                System.exit(1);
            }
            Schema newSchema = new Schema.Parser().parse(new File(schemaPath));
            if (!AvroCompatibilityLevel.BACKWARD.compatibilityChecker.isCompatible(newSchema, defaultSchema)) {
                System.err.println("New schema have to be backward compatible with default. To see default schema run with --dump-default-schema");
                System.exit(1);
            }
            Schema contentSchema = newSchema.getField("content").schema();
            DatumReader<Object> reader = new GenericDatumReader<Object>(contentSchema);
            String input = "{\"n\":42}";
            Decoder decoder = DecoderFactory.get().jsonDecoder(contentSchema, input);
            Object datum = reader.read(null, decoder);
            System.out.println(datum);
            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1);
            int id = schemaRegistryClient.register(schemaSubject, newSchema);
            URI schemaUri = URI.create(schemaRegistryUrl + "/schemas/ids/" + id).normalize();
            System.out.println("Schema has been registered for subject \"" + schemaSubject + "\".\n" +
                    "To use it, specify \"schema.url=" + schemaUri + "\" in connector config.");
        }
    }
}
