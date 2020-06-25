package com.couchbase.connect.kafka.sink;

import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.ArrayAppend;
import com.couchbase.client.java.kv.ArrayPrepend;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.Upsert;
import com.couchbase.connect.kafka.util.DocumentPathExtractor;
import com.couchbase.connect.kafka.util.DurabilitySetter;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;

import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.StoreSemantics.REPLACE;
import static com.couchbase.client.java.kv.StoreSemantics.UPSERT;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class SubDocumentWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SubDocumentWriter.class);

  private static class SubdocOperation {
    private final String id;
    private final String path;
    private final JsonObject data;

    public SubdocOperation(String id, String path, byte[] data) {
      this.id = id;
      this.path = path;
      this.data = JsonObject.fromJson(data);
    }

    public String getId() {
      return id;
    }

    public String getPath() {
      return path;
    }

    public JsonObject getData() {
      return data;
    }
  }

  private final SubDocumentMode mode;
  private final String path;
  private final boolean createPaths;
  private final boolean pathIsDynamic;
  private final boolean createDocuments;
  private final Duration documentExpiry;

  public SubDocumentWriter(SubDocumentMode mode, String path, boolean createPaths, boolean createDocuments, Duration expiry) {
    this.mode = requireNonNull(mode);

    if (path.startsWith("/")) {
      // Interpret the given path as a JSON pointer.
      // Each Kafka message is then expected to have a field at this location;
      // the value of that field is the path to use when doing the subdoc operation.
      this.path = "${" + path + "}";
      this.pathIsDynamic = true;
    } else {
      // Interpret the path as a normal subdoc path.
      // The same path is then used for each subdoc operation.
      this.path = path;
      this.pathIsDynamic = false;
    }

    this.createPaths = createPaths;
    this.createDocuments = createDocuments;
    this.documentExpiry = requireNonNull(expiry);
  }

  public Mono<Void> write(final ReactiveCollection bucket, final JsonBinaryDocument document, DurabilitySetter durabilitySetter) {
    SubdocOperation operation = getOperation(document);

    MutateInSpec mutation;

    switch (mode) {
      case UPSERT:
        mutation = MutateInSpec.upsert(operation.getPath(), operation.getData());
        if (createPaths) {
          mutation = ((Upsert) mutation).createPath();
        }
        break;

      case ARRAY_APPEND:
        mutation = MutateInSpec.arrayAppend(operation.getPath(), singletonList(operation.getData()));
        if (createPaths) {
          mutation = ((ArrayAppend) mutation).createPath();
        }
        break;

      case ARRAY_PREPEND:
        mutation = MutateInSpec.arrayPrepend(operation.getPath(), singletonList(operation.getData()));
        if (createPaths) {
          mutation = ((ArrayPrepend) mutation).createPath();
        }
        break;

      default:
        throw new RuntimeException("Unsupported subdoc mode: " + mode);
    }

    MutateInOptions options = mutateInOptions()
        .expiry(documentExpiry)
        .storeSemantics(createDocuments ? UPSERT : REPLACE);
    durabilitySetter.accept(options);

    return bucket.mutateIn(document.id(), singletonList(mutation), options)
        .then();
  }

  private SubdocOperation getOperation(JsonBinaryDocument doc) {
    if (!pathIsDynamic) {
      return new SubdocOperation(doc.id(), this.path, doc.content());
    }

    try {
      DocumentPathExtractor extractor = new DocumentPathExtractor(path, true);
      DocumentPathExtractor.DocumentExtraction extraction = extractor.extractDocumentPath(doc.content());
      return new SubdocOperation(doc.id(), extraction.getPathValue(), extraction.getData());

    } catch (IOException | DocumentPathExtractor.DocumentPathNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      return new SubdocOperation(doc.id(), null, null);
    }
  }
}
