/*
 * Copyright (c) 2017 Couchbase, Inc.
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

package com.couchbase.connect.kafka;

import com.couchbase.client.core.logging.LogRedaction;
import com.couchbase.client.java.Collection;
import com.couchbase.connect.kafka.config.sink.CouchbaseSinkConfig;
import com.couchbase.connect.kafka.config.sink.SinkBehaviorConfig.DocumentMode;
import com.couchbase.connect.kafka.handler.sink.N1qlSinkHandler;
import com.couchbase.connect.kafka.handler.sink.SinkAction;
import com.couchbase.connect.kafka.handler.sink.SinkDocument;
import com.couchbase.connect.kafka.handler.sink.SinkHandler;
import com.couchbase.connect.kafka.handler.sink.SinkHandlerContext;
import com.couchbase.connect.kafka.handler.sink.SinkHandlerParams;
import com.couchbase.connect.kafka.handler.sink.SubDocumentSinkHandler;
import com.couchbase.connect.kafka.util.BatchBuilder;
import com.couchbase.connect.kafka.util.DocumentIdExtractor;
import com.couchbase.connect.kafka.util.DocumentPathExtractor;
import com.couchbase.connect.kafka.util.DurabilitySetter;
import com.couchbase.connect.kafka.util.KafkaRetryHelper;
import com.couchbase.connect.kafka.util.ScopeAndCollection;
import com.couchbase.connect.kafka.util.TopicMap;
import com.couchbase.connect.kafka.util.Version;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static com.couchbase.client.core.util.CbStrings.removeStart;
import static java.util.Collections.unmodifiableMap;

public class CouchbaseSinkTask extends SinkTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSinkTask.class);

  private ScopeAndCollection defaultDestCollection;
  private Map<String, ScopeAndCollection> topicToCollection;
  private KafkaCouchbaseClient client;
  private JsonConverter converter;
  private DocumentIdExtractor documentIdExtractor;
  private SinkHandler sinkHandler;
  private KafkaRetryHelper retryHelper;

  private DurabilitySetter durabilitySetter;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private Optional<Duration> documentExpiry;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    CouchbaseSinkConfig config;
    try {
      config = ConfigHelper.parse(CouchbaseSinkConfig.class, properties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start CouchbaseSinkTask due to configuration error", e);
    }

    Map<String, String> clusterEnvProperties = new HashMap<>();
    properties.forEach((key, value) -> {
      if (key.startsWith("couchbase.env.") && !isNullOrEmpty(value)) {
        clusterEnvProperties.put(removeStart(key, "couchbase.env."), value);
      }
    });

    LOGGER.info("Custom ClusterEnvironment properties: {}", clusterEnvProperties);

    LogRedaction.setRedactionLevel(config.logRedaction());
    client = new KafkaCouchbaseClient(config, clusterEnvProperties);
    defaultDestCollection = ScopeAndCollection.parse(config.defaultCollection());
    topicToCollection = TopicMap.parseTopicToCollection(config.topicToCollection());

    converter = new JsonConverter();
    converter.configure(mapOf("schemas.enable", false), false);

    String docIdPointer = config.documentId();
    if (docIdPointer != null && !docIdPointer.isEmpty()) {
      documentIdExtractor = new DocumentIdExtractor(docIdPointer, config.removeDocumentId());
    }

    Class<? extends SinkHandler> sinkHandlerClass = config.sinkHandler();

    DocumentMode documentMode = config.documentMode();
    if (documentMode != DocumentMode.DOCUMENT) {
      sinkHandlerClass = documentMode == DocumentMode.N1QL
          ? N1qlSinkHandler.class
          : SubDocumentSinkHandler.class;
      LOGGER.warn("Forcing sink handler to {} because document mode is {}." +
              " The `couchbase.document.mode` config property is deprecated;" +
              " please use `couchbase.sink.handler` instead.",
          sinkHandlerClass, documentMode);
    }

    sinkHandler = Utils.newInstance(sinkHandlerClass);
    sinkHandler.init(new SinkHandlerContext(client.cluster().reactive(), unmodifiableMap(properties)));

    LOGGER.info("Using sink handler: {}", sinkHandler);

    durabilitySetter = DurabilitySetter.create(config);
    documentExpiry = config.documentExpiration().isZero()
        ? Optional.empty()
        : Optional.of(config.documentExpiration());

    retryHelper = new KafkaRetryHelper("CouchbaseSinkTask.put()", config.retryTimeout());

    if (usingLongKvTimeouts()) {
      String retryTimeoutName = ConfigHelper.keyName(CouchbaseSinkConfig.class, CouchbaseSinkConfig::retryTimeout);
      LOGGER.warn("The specified KV timeout is very long, and might cause problems for the Kafka consumer session. " +
          " Consider using the '" + retryTimeoutName + "' config property" +
          " instead of setting a long KV timeout. The retry timeout handles more kinds of write failures" +
          " and can safely be set to a duration longer than Kafka consumer session timeout.");
    }
  }

  private boolean usingLongKvTimeouts() {
    Duration actualKvTimeout = client.cluster().environment().timeoutConfig().kvTimeout();
    Duration actualKvDurableTimeout = client.cluster().environment().timeoutConfig().kvDurableTimeout();

    // something shorter than the default Kafka consumer session timeout of 30 seconds
    Duration threshold = Duration.ofSeconds(20);

    return actualKvTimeout.compareTo(threshold) > 0 ||
        actualKvDurableTimeout.compareTo(threshold) > 0;
  }

  @Override
  public void put(java.util.Collection<SinkRecord> records) {
    retryHelper.runWithRetry(() -> doPut(records));
  }

  private void doPut(java.util.Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    LOGGER.trace("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the Couchbase...",
        recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

    List<SinkAction> actions = new ArrayList<>(records.size());
    for (SinkRecord record : records) {
      ScopeAndCollection destCollectionSpec = topicToCollection.getOrDefault(record.topic(), defaultDestCollection);
      Collection destCollection = client.collection(destCollectionSpec);

      SinkAction action = sinkHandler.handle(
          new SinkHandlerParams(
              client.cluster().reactive(),
              destCollection.reactive(),
              record,
              toSinkDocument(record),
              documentExpiry,
              durabilitySetter));

      if (action != null) {
        actions.add(action);
      }
    }

    execute(actions);
  }

  private static void execute(List<SinkAction> actions) {
    // The Kafka consumer session will probably expire long before this.
    // This is just a failsafe so we don't end up waiting for Godot.
    Duration timeout = Duration.ofMinutes(10);

    toMono(actions).block(timeout);
  }

  // visible for testing
  static Mono<Void> toMono(List<SinkAction> actions) {
    // Use concurrency hints to group the actions into batches
    BatchBuilder<Mono<Void>> batchBuilder = new BatchBuilder<>();
    for (SinkAction action : actions) {
      batchBuilder.add(action.action(), action.concurrencyHint());
    }

    // Transform each batch of actions into a Flux that runs the actions
    // in the batch concurrently (up to the default flatMap concurrency limit).
    Stream<Mono<Void>> batches = batchBuilder.build().stream()
        .map(batch -> Flux.fromIterable(batch)
            .flatMap(it -> it)
            .then()); // Just for clarity, convert the Flux<Void> into a Mono<Void>.

    // Now we have a stream of Mono<Void>s where each mono represents a batch.
    // Concatenate them so we end up waiting for each batch to complete
    // before starting the next one.
    return Flux.fromStream(batches)
        .concatMap(it -> it)
        .then(); // We only care about the final completion signal.
  }

  /**
   * @return (nullable)
   */
  private SinkDocument toSinkDocument(SinkRecord record) {
    if (record.value() == null) {
      return null;
    }

    byte[] valueAsJsonBytes = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
    try {
      if (documentIdExtractor != null) {
        return documentIdExtractor.extractDocumentId(valueAsJsonBytes);
      }

    } catch (DocumentPathExtractor.DocumentPathNotFoundException e) {
      LOGGER.warn(e.getMessage() + "; letting sink handler use fallback ID");

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new SinkDocument(null, valueAsJsonBytes);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
  }

  @Override
  public void stop() {
    if (retryHelper != null) {
      retryHelper.close();
      retryHelper = null;
    }

    if (client != null) {
      client.close();
      client = null;
    }
  }
}
