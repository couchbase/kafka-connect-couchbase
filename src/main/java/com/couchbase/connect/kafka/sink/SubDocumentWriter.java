package com.couchbase.connect.kafka.sink;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.subdoc.AsyncMutateInBuilder;
import com.couchbase.client.java.subdoc.SubdocOptionsBuilder;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;

import rx.functions.Action1;
import rx.functions.Func1;

import static com.couchbase.client.deps.io.netty.util.CharsetUtil.UTF_8;

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
                case ARRAY_INSERT: {
                    mutation = mutation.arrayInsert(path, node, options);
                    break;
                }
                case ARRAY_APPEND: {
                    mutation = mutation.arrayAppend(path, node, options);

                    break;
                }
                case ARRAY_PREPEND: {
                    mutation = mutation.arrayPrepend(path, node, options);

                    break;
                }
                case ARRAY_INSERT_ALL: {
                    mutation = mutation.arrayInsertAll(path, node, options);

                    break;
                }
                case ARRAY_APPEND_ALL: {
                    mutation = mutation.arrayAppendAll(path, node, options);

                    break;
                }
                case ARRAY_PREPEND_ALL: {
                    mutation = mutation.arrayPrependAll(path, node, options);
                    break;
                }
                case ARRAY_ADD_UNIQUE: {
                    mutation = mutation.arrayAddUnique(path, node, options);
                    break;
                }
            }
        }

         if(createDocuments) {
             return mutation.execute(persistTo, replicateTo)
                     .toCompletable()
                     .retryWhen(
                             RetryBuilder.anyOf(DocumentDoesNotExistException.class)
                                     .once()
                                     .build()
                     )
                     .onErrorResumeNext(new Func1<Throwable, Completable>() {
                         @Override
                         public Completable call(Throwable throwable) {
                             if (createDocuments && throwable instanceof DocumentDoesNotExistException) {
                                 return bucket.insert(JsonDocument.create(document.id())).toCompletable();
                             } else {
                                 return Completable.error(throwable);
                             }
                         }
                     });
         }
         else {
             return mutation.execute(persistTo, replicateTo)
                     .toCompletable();
         }
    }
}

