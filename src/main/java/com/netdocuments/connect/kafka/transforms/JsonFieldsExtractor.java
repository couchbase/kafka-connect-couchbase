package com.netdocuments.connect.kafka.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.connect.kafka.util.JsonPropertyExtractor;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class JsonFieldsExtractor<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger log = LoggerFactory.getLogger(JsonFieldsExtractor.class);

  public static final String OVERVIEW_DOC = "Extracts specified fields from a JSON message";

  public static final String FIELDS_CONFIG = "fields";
  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(FIELDS_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
          "Comma-separated list of JSON fields to extract");

  private String[] fields;
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> props) {
    final AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
    fields = config.getString(FIELDS_CONFIG).split(",");
    // fields = new String[] { "envProps.acl", "documents.1.docProps.id",
    // "envProps.url" };
  }

  @Override
  public R apply(R record) {
    final Object value = record.value();
    final Map newValue;
    try {
      if (value == null) {
        return record;
      } else if (value instanceof byte[]) {
        newValue = JsonPropertyExtractor.extract(new ByteArrayInputStream((byte[]) value), fields);
        log.debug("Extracted fields: {}", newValue);
      } else if (value instanceof ByteBuffer) {
        try (ByteBufferInputStream in = new ByteBufferInputStream((ByteBuffer) value)) {
          newValue = JsonPropertyExtractor.extract(in, fields);
          log.debug("Extracted fields: {}", newValue);
        }
      } else {
        throw new DataException(getClass().getSimpleName()
            + " transform expected value to be a byte array or ByteBuffer but got " + value.getClass().getName());
      }

      byte[] newValueBytes;
      if (newValue.isEmpty()) {
        return null;
      } else {
        newValueBytes = objectMapper.writeValueAsBytes(newValue);
      }

      R newRecord = record.newRecord(record.topic(), record.kafkaPartition(),
          record.keySchema(), record.key(),
          record.valueSchema(), newValueBytes,
          record.timestamp());

      return newRecord;

    } catch (IOException e) {
      throw new DataException(
          getClass().getSimpleName() + " transform expected value to be JSON but got something else.", e);
    } catch (Exception e) {
      throw new DataException(getClass().getSimpleName() + " transform failed to extract fields from JSON.", e);
    }
  }

  @Override
  public void close() {
    // Nothing to do here
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }
}
