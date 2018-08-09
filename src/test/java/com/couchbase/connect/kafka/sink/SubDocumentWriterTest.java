package com.couchbase.connect.kafka.sink;

import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.subdoc.CannotInsertValueException;
import com.couchbase.client.java.subdoc.AsyncMutateInBuilder;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.SubdocOptionsBuilder;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import rx.Completable;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.deps.io.netty.util.CharsetUtil.UTF_8;


import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SubDocumentWriterTest {

    private SubDocumentWriter writer;

    private final String path = "leaf";

    @Mock
    private AsyncBucket bucket;

    @Mock
    private AsyncMutateInBuilder mutateInBuilder;

    @Captor
    private ArgumentCaptor<String> mutateInArg;

    Observable<DocumentFragment<Mutation>> emptyResult = Observable.empty();

    @Before
    public void before(){
        Observable<DocumentFragment<Mutation>> result = Observable.empty();
        List<JsonDocument> documents = new ArrayList();
        documents.add(JsonDocument.create("id"));

        Observable<JsonDocument> insert = Observable.from(documents);

        Mockito.when(bucket.name()).thenReturn("default");
        Mockito.when(bucket.mutateIn(Mockito.any(String.class))).thenReturn(mutateInBuilder);
        Mockito.when(bucket.insert(Mockito.any(JsonDocument.class))).thenReturn(insert);
        Mockito.when(mutateInBuilder.upsertDocument(Mockito.anyBoolean()))
                .thenReturn(mutateInBuilder);
    }

    private Completable write(JsonObject object, SubDocumentMode mode){
        return write(object,mode,path, false, emptyResult);
    }

    private Completable write(JsonObject object, String path, boolean extractPath, SubDocumentMode mode){
        return write(object,mode, path, extractPath, emptyResult);
    }

    private Completable write(JsonObject object, SubDocumentMode mode, String path,  boolean extractPath, Observable<DocumentFragment<Mutation>> result){
        Mockito.when(mutateInBuilder.execute(Mockito.any(PersistTo.class),Mockito.any(ReplicateTo.class))).thenReturn(result);

        writer = new SubDocumentWriter(mode,path,extractPath, true,true);

        JsonBinaryDocument document = null;
        if(object != null){
            document = JsonBinaryDocument.create("id", object.toString().getBytes(UTF_8));
        }

        return  writer.write(bucket, document, PersistTo.NONE, ReplicateTo.NONE);
    }


    @Test
    public void doesNotGenerateStatementOnNull()  {
        write(null, SubDocumentMode.UPSERT);

        verify(bucket, never()).mutateIn(mutateInArg.capture());
    }

    @Test
    public void upsertsPathWithEmptyJsonObject() {

        Mockito.when(mutateInBuilder.upsert(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(JsonObject.empty(), SubDocumentMode.UPSERT);
        verify(bucket).mutateIn(mutateInArg.capture());

        r.await();
    }

    @Test
    public void upsertsPathWithJsonObject() {
        JsonObject object = JsonObject.create();

        Mockito.when(mutateInBuilder.upsert(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(object, SubDocumentMode.UPSERT);

        verify(bucket).mutateIn(mutateInArg.capture());
        verify(mutateInBuilder).upsert(Mockito.eq(path), Mockito.eq(object), Mockito.any(SubdocOptionsBuilder.class));

        r.await();
    }

    @Test
    public void insertsToArray() {
        JsonObject object = JsonObject.create();

        Mockito.when(mutateInBuilder.arrayInsert(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(object, SubDocumentMode.ARRAY_INSERT);

        verify(bucket).mutateIn(mutateInArg.capture());
        verify(mutateInBuilder).arrayInsert(Mockito.eq(path), Mockito.eq(object), Mockito.any(SubdocOptionsBuilder.class));

        r.await();
    }

    @Test
    public void appendsToArray() {
        JsonObject object = JsonObject.create();

        Mockito.when(mutateInBuilder.arrayAppend(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(object, SubDocumentMode.ARRAY_APPEND);

        verify(bucket).mutateIn(mutateInArg.capture());
        verify(mutateInBuilder).arrayAppend(Mockito.eq(path), Mockito.eq(object), Mockito.any(SubdocOptionsBuilder.class));

        r.await();
    }

    @Test
    public void prependsToArray() {
        JsonObject object = JsonObject.create();

        Mockito.when(mutateInBuilder.arrayPrepend(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(object, SubDocumentMode.ARRAY_PREPEND);

        verify(bucket).mutateIn(mutateInArg.capture());
        verify(mutateInBuilder).arrayPrepend(Mockito.eq(path), Mockito.eq(object), Mockito.any(SubdocOptionsBuilder.class));

        r.await();
    }

    @Test
    public void insertsAllToArray() {
        JsonObject object = JsonObject.create();

        Mockito.when(mutateInBuilder.arrayInsertAll(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(object, SubDocumentMode.ARRAY_INSERT_ALL);

        verify(bucket).mutateIn(mutateInArg.capture());
        verify(mutateInBuilder).arrayInsertAll(Mockito.eq(path), Mockito.eq(object), Mockito.any(SubdocOptionsBuilder.class));

        r.await();
    }

    @Test
    public void appendsAllToArray() {
        JsonObject object = JsonObject.create();

        Mockito.when(mutateInBuilder.arrayAppendAll(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(object, SubDocumentMode.ARRAY_APPEND_ALL);

        verify(bucket).mutateIn(mutateInArg.capture());
        verify(mutateInBuilder).arrayAppendAll(Mockito.eq(path), Mockito.eq(object), Mockito.any(SubdocOptionsBuilder.class));

        r.await();
    }

    @Test
    public void prependsAllToArray() {
        JsonObject object = JsonObject.create();

        Mockito.when(mutateInBuilder.arrayPrependAll(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(object, SubDocumentMode.ARRAY_PREPEND_ALL);

        verify(bucket).mutateIn(mutateInArg.capture());
        verify(mutateInBuilder).arrayPrependAll(Mockito.eq(path), Mockito.eq(object), Mockito.any(SubdocOptionsBuilder.class));

        r.await();
    }

    @Test
    public void addsToArrayUnique() {
        JsonObject object = JsonObject.create();

        Mockito.when(mutateInBuilder.arrayAddUnique(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(object, SubDocumentMode.ARRAY_ADD_UNIQUE);

        verify(bucket).mutateIn(mutateInArg.capture());
        verify(mutateInBuilder).arrayAddUnique(Mockito.eq(path), Mockito.eq(object), Mockito.any(SubdocOptionsBuilder.class));

        r.await();

    }

    @Test
    public void extractsPathFromDocument() {
        JsonObject object = JsonObject.create();
        object.put("path","leaf");

        JsonObject result = JsonObject.create();

        Mockito.when(mutateInBuilder.arrayAddUnique(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(object,  "/path", true, SubDocumentMode.ARRAY_ADD_UNIQUE);

        verify(bucket).mutateIn(mutateInArg.capture());
        verify(mutateInBuilder).arrayAddUnique(Mockito.eq(path), Mockito.eq(result), Mockito.any(SubdocOptionsBuilder.class));

        r.await();
    }

    @Test(expected = DocumentDoesNotExistException.class)
    public void createsDocumentOnDocumentDoesNotExistException() {
        Observable<DocumentFragment<Mutation>> error = Observable.error(new DocumentDoesNotExistException());

        JsonObject object = JsonObject.create();

        Mockito.when(mutateInBuilder.arrayAddUnique(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(object, SubDocumentMode.ARRAY_ADD_UNIQUE,"leaf",false,  error);

        verify(bucket).mutateIn(mutateInArg.capture());
        verify(mutateInBuilder).arrayAddUnique(Mockito.eq(path), Mockito.eq(object), Mockito.any(SubdocOptionsBuilder.class));

        r.await();

        verify(bucket).insert(Mockito.any(JsonDocument.class));
    }

    @Test(expected = CannotInsertValueException.class)
    public void ignoresOtherThrowables() {
        Observable<DocumentFragment<Mutation>> error = Observable.error(new CannotInsertValueException(path));

        JsonObject object = JsonObject.create();

        Mockito.when(mutateInBuilder.arrayAddUnique(Mockito.any(String.class), Mockito.any(JsonObject.class), Mockito.any(SubdocOptionsBuilder.class)))
                .thenReturn(mutateInBuilder);

        Completable r = write(object, SubDocumentMode.ARRAY_ADD_UNIQUE, "leaf",false, error);

        verify(bucket).mutateIn(mutateInArg.capture());
        verify(mutateInBuilder).arrayAddUnique(Mockito.eq(path), Mockito.eq(object), Mockito.any(SubdocOptionsBuilder.class));

        r.await();

        verify(bucket, never()).insert(Mockito.any(JsonDocument.class));
    }

}
