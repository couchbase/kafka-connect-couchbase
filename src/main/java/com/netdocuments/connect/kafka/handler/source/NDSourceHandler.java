/*
 * Copyright 2024 NetDocuments Software, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netdocuments.connect.kafka.handler.source;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.handler.source.RawJsonWithMetadataSourceHandler;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import com.couchbase.connect.kafka.handler.source.SourceRecordBuilder;
import com.couchbase.connect.kafka.util.JsonPropertyExtractor;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

/**
 * NDSourceHandler extends RawJsonWithMetadataSourceHandler to provide custom
 * handling
 * for Couchbase documents, including field extraction, filtering, and S3 upload
 * capabilities.
 * It supports CloudEvents format and can be configured to filter documents
 * based on key patterns and document types.
 */
public class NDSourceHandler extends RawJsonWithMetadataSourceHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NDSourceHandler.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Configuration keys
  private static final String FIELDS_CONFIG = "couchbase.custom.handler.nd.fields";
  private static final String OUTPUT_FORMAT = "couchbase.custom.handler.nd.output.format";
  private static final String S3_BUCKET_CONFIG = "couchbase.custom.handler.nd.s3.bucket";
  private static final String S3_REGION_CONFIG = "couchbase.custom.handler.nd.s3.region";

  // Configuration definition
  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(FIELDS_CONFIG, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH,
          "The fields to extract from the document")
      .define(OUTPUT_FORMAT, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
          "The output format of the message. The only current valid value is 'cloudevent' anything else designates the default format")
      .define(S3_BUCKET_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
          "The S3 bucket to upload documents to")
      .define(S3_REGION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
          "The AWS region for the S3 bucket");

  private List<String> fields;
  private boolean cloudevent;
  private S3Client s3Client;
  private String s3Bucket;
  private boolean isS3Enabled;

  /**
   * Initializes the handler with the given configuration properties.
   * This method is called when the connector starts up.
   *
   * @param configProperties The configuration properties for the handler
   */
  @Override
  public void init(Map<String, String> configProperties) {
    super.init(configProperties);
    AbstractConfig config = new AbstractConfig(CONFIG_DEF, configProperties);

    initializeHandlerProperties(config);
    initializeS3Client(config);
  }

  /**
   * Initializes the handler properties including fields, types, key pattern, and
   * CloudEvent settings.
   */
  private void initializeHandlerProperties(AbstractConfig config) {
    // Initialize fields
    fields = config.getList(FIELDS_CONFIG);

    // Initialize CloudEvent setting
    String cloudEventRaw = config.getString(OUTPUT_FORMAT);
    cloudevent = "cloudevent".equals(cloudEventRaw);
  }

  /**
   * Initializes the S3 client for document uploads.
   */
  private void initializeS3Client(AbstractConfig config) {
    s3Bucket = config.getString(S3_BUCKET_CONFIG);
    String s3Region = config.getString(S3_REGION_CONFIG);
    if (s3Bucket == null || s3Region == null) {
      isS3Enabled = false;
    } else {
      isS3Enabled = true;
      s3Client = S3Client.builder()
          .region(Region.of(s3Region))
          .build();
    }
  }

  /**
   * Handles a document event and builds a SourceRecord.
   *
   * @param params The parameters containing the document event and other context
   * @return A SourceRecordBuilder with the processed event, or null if the event
   *         should be skipped
   */
  @Override
  public SourceRecordBuilder handle(SourceHandlerParams params) {
    SourceRecordBuilder builder = new SourceRecordBuilder();
    if (cloudevent) {
      addCloudEventHeaders(builder);
    }

    if (!buildValue(params, builder)) {
      return null;
    }

    return builder
        .topic(getTopic(params))
        .key(Schema.STRING_SCHEMA, params.documentEvent().key());
  }

  /**
   * Adds CloudEvent-specific headers to the SourceRecordBuilder.
   */
  private void addCloudEventHeaders(SourceRecordBuilder builder) {
    builder.headers().addString("ce_specversion", "1.0");
    builder.headers().addString("content-type", "application/cloudevents");
  }

  /**
   * Builds the value for the SourceRecord based on the document event.
   *
   * @param params  The parameters containing the document event and other context
   * @param builder The SourceRecordBuilder to populate
   * @return true if the value was successfully built, false otherwise
   */
  @Override
  protected boolean buildValue(SourceHandlerParams params, SourceRecordBuilder builder) {
    if (fields.isEmpty() && !cloudevent && !isS3Enabled) {
      return super.buildValue(params, builder);
    }

    final DocumentEvent docEvent = params.documentEvent();
    final DocumentEvent.Type type = docEvent.type();

    if (fields.size() == 1 && fields.get(0).equals("*")) {
      return handleAllFieldsExtraction(docEvent, type, params, builder);
    }

    return handleSpecificFieldsExtraction(docEvent, type, params, builder);
  }

  /**
   * Handles extraction of all fields from the document.
   */
  private boolean handleAllFieldsExtraction(DocumentEvent docEvent, DocumentEvent.Type type, SourceHandlerParams params,
      SourceRecordBuilder builder) {
    switch (type) {
      case EXPIRATION:
      case DELETION:
        return handleDeletionOrExpiration(docEvent, type, builder);
      case MUTATION:
        return handleMutation(docEvent, params, builder);
      default:
        LOGGER.warn("unexpected event type {}", type);
        return false;
    }
  }

  /**
   * Handles deletion or expiration events.
   */
  private boolean handleDeletionOrExpiration(DocumentEvent docEvent, DocumentEvent.Type type,
      SourceRecordBuilder builder) {
    Map<String, Object> newValue = new HashMap<>();
    newValue.put("event", type.schemaName());
    newValue.put("key", docEvent.key());
    try {
      byte[] value = convertToBytes(newValue, docEvent);
      builder.value(null, value);
      return true;
    } catch (DataException e) {
      LOGGER.error("Failed to serialize data", e);
      return false;
    }
  }

  /**
   * Handles mutation events, including uploading to S3.
   */
  private boolean handleMutation(DocumentEvent docEvent, SourceHandlerParams params, SourceRecordBuilder builder) {
    if (params.noValue()) {
      builder.value(null, convertToBytes(null, docEvent));
      return true;
    }

    final byte[] document = docEvent.content();
    if (!isValidJson(document)) {
      LOGGER.warn("Skipping non-JSON document: bucket={} key={}", docEvent.bucket(), docEvent.qualifiedKey());
      return false;
    }

    uploadToS3(docEvent, document);

    if (cloudevent) {
      builder.value(null, withCloudEvent(document, docEvent));
    } else {
      builder.value(null, document);
    }
    return true;
  }

  /**
   * Uploads the document content to S3.
   */
  private void uploadToS3(DocumentEvent docEvent, byte[] document) {
    if (!isS3Enabled) {
      return;
    }
    String s3Key = generateS3Key(docEvent);
    try {
      PutObjectRequest putObjectRequest = PutObjectRequest.builder()
          .bucket(s3Bucket)
          .key(s3Key)
          .contentType("application/json")
          .build();

      s3Client.putObject(putObjectRequest, RequestBody.fromBytes(document));
      LOGGER.info("Uploaded document to S3: s3://{}/{}", s3Bucket, s3Key);
    } catch (Exception e) {
      LOGGER.error("Failed to upload document to S3: {}", e.getMessage(), e);
    }
  }

  /**
   * Generates a unique S3 key for the document.
   */
  private String generateS3Key(DocumentEvent docEvent) {
    return String.format("%s/%s/%s.json", docEvent.bucket(), Instant.now().toString().replace(":", "-"),
        docEvent.key());
  }

  /**
   * Handles extraction of specific fields from the document.
   */
  private boolean handleSpecificFieldsExtraction(DocumentEvent docEvent, DocumentEvent.Type type,
      SourceHandlerParams params, SourceRecordBuilder builder) {
    final byte[] content = docEvent.content();
    final Map<String, Object> newValue;

    if (type == DocumentEvent.Type.DELETION || type == DocumentEvent.Type.EXPIRATION) {
      newValue = createDeletionOrExpirationValue(docEvent, type);
    } else if (type == DocumentEvent.Type.MUTATION) {
      newValue = createMutationValue(docEvent, content);
      if (newValue == null) {
        return false;
      }
    } else {
      LOGGER.warn("unexpected event type {}", type);
      return false;
    }

    try {
      builder.value(null, convertToBytes(newValue, docEvent));
      return true;
    } catch (DataException e) {
      LOGGER.error("Failed to serialize data", e);
      return false;
    }
  }

  /**
   * Creates a value map for deletion or expiration events.
   */
  private Map<String, Object> createDeletionOrExpirationValue(DocumentEvent docEvent, DocumentEvent.Type type) {
    Map<String, Object> newValue = new HashMap<>();
    newValue.put("event", type.schemaName());
    newValue.put("key", docEvent.key());
    return newValue;
  }

  /**
   * Creates a value map for mutation events, extracting specified fields.
   */
  private Map<String, Object> createMutationValue(DocumentEvent docEvent, byte[] content) {
    try {
      Map<String, Object> newValue = JsonPropertyExtractor.extract(new ByteArrayInputStream(content),
          fields.toArray(new String[fields.size()]));
      newValue.put("event", DocumentEvent.Type.MUTATION.schemaName());
      newValue.put("key", docEvent.key());
      return newValue;
    } catch (Exception e) {
      LOGGER.error("Error while extracting fields from document", e);
      return null;
    }
  }

  /**
   * Converts a value to bytes, applying CloudEvent format if necessary.
   */
  private byte[] convertToBytes(Map<String, Object> value, DocumentEvent docEvent) {
    if (!cloudevent) {
      return serializeToJson(value);
    }
    return withCloudEvent(serializeToJson(value), docEvent);
  }

  /**
   * Serializes an object to JSON bytes.
   */
  private byte[] serializeToJson(Object value) {
    try {
      return OBJECT_MAPPER.writeValueAsBytes(value);
    } catch (IOException e) {
      throw new DataException("Failed to serialize data", e);
    }
  }

  /**
   * Wraps the given value in a CloudEvent format.
   */
  private byte[] withCloudEvent(byte[] value, DocumentEvent documentEvent) {
    Map<String, Object> cloudEventData = createCloudEventData(documentEvent);
    byte[] cloudEventBytes = serializeToJson(cloudEventData);

    ByteArrayBuilder result = new ByteArrayBuilder(
        cloudEventBytes.length + ",\"data\":".getBytes().length + value.length)
        .append(cloudEventBytes, cloudEventBytes.length - 1)
        .append(",\"data\":".getBytes())
        .append(value)
        .append((byte) '}');
    return result.build();
  }

  /**
   * Creates the CloudEvent metadata for a document event.
   */
  private Map<String, Object> createCloudEventData(DocumentEvent documentEvent) {
    Map<String, Object> data = new HashMap<>();
    data.put("specversion", "1.0");
    data.put("id", documentEvent.key() + "-" + documentEvent.revisionSeqno());
    data.put("type", "com.netdocuments.ndserver." + documentEvent.bucket() + "." + documentEvent.type().schemaName());
    data.put("source", "netdocs://ndserver/" + documentEvent.bucket());
    data.put("time", Instant.now().toString());
    data.put("datacontenttype", "application/json;charset=utf-8");
    data.put("partitionkey", documentEvent.key());
    data.put("traceparent", UUID.randomUUID().toString());
    return data;
  }

  // For testing purposes
  void setS3Client(S3Client s3Client) {
    this.s3Client = s3Client;
  }
}
