package com.couchbase.connect.kafka.sink;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.subdoc.AsyncMutateInBuilder;
import com.couchbase.client.java.subdoc.SubdocOptionsBuilder;
import com.couchbase.connect.kafka.util.DocumentPathExtractor;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;

import java.io.IOException;

import static com.couchbase.client.deps.io.netty.util.CharsetUtil.UTF_8;

public class SubDocumentWriter {

    private static class SubdocOperation {
        private String id;
        private String path;
        private JsonObject data;

        public SubdocOperation(String id, String path, ByteBuf data) {
            this.id = id;
            this.path = path;
            this.data = JsonObject.fromJson(data.toString(UTF_8));
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

    private static final Logger LOGGER = LoggerFactory.getLogger(SubDocumentWriter.class);

    private SubDocumentMode mode;

    private String path;

    private boolean createPaths;

    private boolean extractPath;

    private boolean createDocuments;

    public SubDocumentWriter(SubDocumentMode mode, String path, boolean extractPath, boolean createPaths, boolean createDocuments) {

        this.mode = mode;
        this.path = path;
        this.extractPath = extractPath;
        this.createPaths = createPaths;
        this.createDocuments = createDocuments;
    }

    public Completable write(final AsyncBucket bucket, final JsonBinaryDocument document, PersistTo persistTo, ReplicateTo replicateTo) {
        if (document == null || (document.content() == null && (document.id() == null || document.id().isEmpty()))) {

            LOGGER.warn("document or document content is null");
            // skip it
            return Completable.complete();
        }

        SubdocOperation operation = getOperation(document);

        SubdocOptionsBuilder options = new SubdocOptionsBuilder().createPath(createPaths);

        AsyncMutateInBuilder mutation = bucket
                .mutateIn(document.id());

        if (operation.data == null && !document.id().isEmpty()) {
            mutation = mutation.remove(operation.path, options);
        } else {
            switch (mode) {
                case UPSERT: {
                    mutation = mutation.upsert(operation.getPath(), operation.getData(), options);
                    break;
                }
                case ARRAY_INSERT: {
                    mutation = mutation.arrayInsert(operation.getPath(), operation.getData(), options);
                    break;
                }
                case ARRAY_APPEND: {
                    mutation = mutation.arrayAppend(operation.getPath(), operation.getData(), options);

                    break;
                }
                case ARRAY_PREPEND: {
                    mutation = mutation.arrayPrepend(operation.getPath(), operation.getData(), options);

                    break;
                }
                case ARRAY_INSERT_ALL: {
                    mutation = mutation.arrayInsertAll(operation.getPath(), operation.getData(), options);

                    break;
                }
                case ARRAY_APPEND_ALL: {
                    mutation = mutation.arrayAppendAll(operation.getPath(), operation.getData(), options);

                    break;
                }
                case ARRAY_PREPEND_ALL: {
                    mutation = mutation.arrayPrependAll(operation.getPath(), operation.getData(), options);
                    break;
                }
                case ARRAY_ADD_UNIQUE: {
                    mutation = mutation.arrayAddUnique(operation.getPath(), operation.getData(), options);
                    break;
                }
            }
        }

        return mutation
                .upsertDocument(createDocuments)
                .execute(persistTo, replicateTo)
                .toCompletable();

    }

    private SubdocOperation getOperation(JsonBinaryDocument doc) {
        String id = doc.id();
        String documentPath = null;
        ByteBuf data = null;
        if (extractPath) {
            DocumentPathExtractor extractor = new DocumentPathExtractor(path, true);
            try {
                DocumentPathExtractor.DocumentExtraction extraction = extractor.extractDocumentPath(getBytes(doc.content()));
                documentPath = extraction.getPathValue();
                data = extraction.getData();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            } catch (DocumentPathExtractor.DocumentPathNotFoundException e) {
                LOGGER.error(e.getMessage(), e);
            }
        } else {
            documentPath = this.path;
            data = doc.content();
        }

        return new SubdocOperation(id, documentPath, data);
    }

    private byte[] getBytes(final ByteBuf byteBuf) {
        final byte[] documentBytes;
        if (byteBuf.hasArray()
                && byteBuf.arrayOffset() == 0
                && byteBuf.readableBytes() == byteBuf.array().length) {
            documentBytes = byteBuf.array();
        } else {
            documentBytes = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(byteBuf.readerIndex(), documentBytes);
        }

        return documentBytes;
    }
}
