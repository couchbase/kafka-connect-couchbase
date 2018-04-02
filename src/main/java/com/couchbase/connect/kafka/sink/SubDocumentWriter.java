package com.couchbase.connect.kafka.sink;

import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonFactory;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.deps.com.fasterxml.jackson.core.TreeNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.subdoc.AsyncMutateInBuilder;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.SubdocOptionsBuilder;
import com.couchbase.connect.kafka.CouchbaseSinkConnectorConfig;
import com.couchbase.connect.kafka.CouchbaseSinkTaskConfig;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.couchbase.connect.kafka.sink.SubDocumentMode;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import static com.couchbase.client.deps.io.netty.util.CharsetUtil.UTF_8;
import static com.couchbase.connect.kafka.CouchbaseSinkConnectorConfig.SUBDOCUMENT_MODE_CONFIG;

public class SubDocumentWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubDocumentWriter.class);

    private SubDocumentMode mode;

    private String path;

    private boolean createPaths;

    private boolean createDocuments;

    public SubDocumentWriter(SubDocumentMode mode, String path, boolean createPaths, boolean createDocuments) {

        this.mode = mode;
        this.path = path;
        this.createPaths = createPaths;
        this.createDocuments = createDocuments;
    }

    public Completable write(final AsyncBucket bucket, final JsonBinaryDocument document, PersistTo persistTo, ReplicateTo replicateTo) {
        if (document == null || (document.content() == null && (document.id() == null || document.id().isEmpty()))) {

            LOGGER.warn("document or document content is null");
            // skip it
            return Completable.complete();
        }

        JsonObject node = JsonObject.fromJson(document.content().toString(UTF_8));

        SubdocOptionsBuilder options = new SubdocOptionsBuilder().createPath(createPaths);

        AsyncMutateInBuilder mutation = bucket
                .mutateIn(document.id());

        if (document.content() == null && !document.id().isEmpty()) {
            mutation = mutation.remove(path, options);
        }
        else {
            switch (mode) {
                case UPSERT: {
                    mutation = mutation.upsert(path, node, options);
                    break;
                }
                case ARRAYINSERT: {
                    mutation = mutation.arrayInsert(path, node, options);
                    break;
                }
                case ARRAYAPPEND: {
                    mutation = mutation.arrayAppend(path, node, options);

                    break;
                }
                case ARRAYPREPEND: {
                    mutation = mutation.arrayPrepend(path, node, options);

                    break;
                }
                case ARRAYINSERTALL: {
                    mutation = mutation.arrayInsertAll(path, node, options);

                    break;
                }
                case ARRAYAPPENDALL: {
                    mutation = mutation.arrayAppendAll(path, node, options);

                    break;
                }
                case ARRAYPREPENDALL: {
                    mutation = mutation.arrayPrependAll(path, node, options);
                    break;
                }
                case ARRAYADDUNIQUE: {
                    mutation = mutation.arrayAddUnique(path, node, options);
                    break;
                }
            }
        }

        return mutation
                .execute(persistTo, replicateTo)
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        if(createDocuments && DocumentDoesNotExistException.class.isInstance(throwable)) {
                            bucket.insert(JsonDocument.create(document.id())).toBlocking().single();
                        }
                    }
                })
                .toCompletable();
    }
}

