package com.netdocuments.connect.kafka.transforms;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetKafkaPartitionFromKey<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(SetKafkaPartitionFromKey.class);

    public static final String PARTITIONS_CONFIG = "partitions";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PARTITIONS_CONFIG, ConfigDef.Type.INT, ConfigDef.Importance.HIGH,
                    "Number of partitions in the target topic");

    private int partitions;

    @Override
    public void configure(Map<String, ?> configs) {
        final AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
        partitions = config.getInt(PARTITIONS_CONFIG);
    }

    @Override
    public R apply(R record) {
        final int partition = Math.abs(record.key().hashCode() % partitions);
        log.trace("Setting partition for {} to {}", record.key(), partition);
        return record.newRecord(record.topic(), partition, record.keySchema(), record.key(),
                record.valueSchema(), record.value(), record.timestamp(), record.headers());
    }

    @Override
    public ConfigDef config() {
        // No configuration needed
        return new ConfigDef();
    }

    @Override
    public void close() {
        // Nothing to do
    }

}
