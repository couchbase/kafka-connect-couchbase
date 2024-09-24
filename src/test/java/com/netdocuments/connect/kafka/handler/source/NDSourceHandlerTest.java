package com.netdocuments.connect.kafka.handler.source;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import com.couchbase.connect.kafka.handler.source.SourceRecordBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NDSourceHandlerTest {

    private NDSourceHandler handler;

    @Mock
    private S3Client mockS3Client;

    private ObjectMapper objectMapper;

    void setUpNonS3() {
        handler = new NDSourceHandler();
        objectMapper = new ObjectMapper();

        Map<String, String> config = new HashMap<>();
        config.put("couchbase.custom.handler.nd.fields", "field1,field2,type");
        config.put("couchbase.custom.handler.nd.output.format", "cloudevent");

        handler.init(config);
        handler.setS3Client(mockS3Client);
    }

    void setUpS3() {
        handler = new NDSourceHandler();
        objectMapper = new ObjectMapper();

        Map<String, String> config = new HashMap<>();
        config.put("couchbase.custom.handler.nd.fields", "*");
        config.put("couchbase.custom.handler.nd.output.format", "cloudevent");
        config.put("couchbase.custom.handler.nd.s3.bucket", "test-bucket");
        config.put("couchbase.custom.handler.nd.s3.region", "us-west-2");

        handler.init(config);
        handler.setS3Client(mockS3Client);
    }

    @Test
    void testHandleMutation() throws Exception {
        setUpNonS3();
        DocumentEvent mockEvent = mock(DocumentEvent.class);
        when(mockEvent.type()).thenReturn(DocumentEvent.Type.MUTATION);
        when(mockEvent.key()).thenReturn("test-key");
        when(mockEvent.content())
                .thenReturn("{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}".getBytes());

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals("test-key", result.key());

        byte[] value = (byte[]) result.value();
        JsonNode jsonNode = objectMapper.readTree(value);

        assertTrue(jsonNode.has("data"));
        JsonNode data = jsonNode.get("data");
        assertEquals("value1", data.get("field1").asText());
        assertEquals("value2", data.get("field2").asText());
        assertEquals("type1", data.get("type").asText());

    }

    @Test
    void testHandleDeletion() throws Exception {
        setUpNonS3();
        DocumentEvent mockEvent = mock(DocumentEvent.class);
        when(mockEvent.type()).thenReturn(DocumentEvent.Type.DELETION);
        when(mockEvent.key()).thenReturn("test-key");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals(Schema.STRING_SCHEMA, result.keySchema());
        assertEquals("test-key", result.key());

        byte[] value = (byte[]) result.value();
        JsonNode jsonNode = objectMapper.readTree(value);

        assertTrue(jsonNode.has("data"));
        JsonNode data = jsonNode.get("data");
        assertEquals("deletion", data.get("event").asText());
        assertEquals("test-key", data.get("key").asText());

        verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testHandleExpiration() throws Exception {
        setUpNonS3();
        DocumentEvent mockEvent = mock(DocumentEvent.class);
        when(mockEvent.type()).thenReturn(DocumentEvent.Type.EXPIRATION);
        when(mockEvent.key()).thenReturn("test-key");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals(Schema.STRING_SCHEMA, result.keySchema());
        assertEquals("test-key", result.key());

        byte[] value = (byte[]) result.value();
        JsonNode jsonNode = objectMapper.readTree(value);

        assertTrue(jsonNode.has("data"));
        JsonNode data = jsonNode.get("data");
        assertEquals("expiration", data.get("event").asText());
        assertEquals("test-key", data.get("key").asText());

        verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testCloudEventHeaders() {
        setUpNonS3();
        DocumentEvent mockEvent = mock(DocumentEvent.class);
        when(mockEvent.type()).thenReturn(DocumentEvent.Type.MUTATION);
        when(mockEvent.key()).thenReturn("test-key");
        when(mockEvent.content())
                .thenReturn("{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}".getBytes());

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        Iterable<Header> headers = result.headers();
        boolean foundSpecVersion = false;
        boolean foundContentType = false;

        for (Header header : headers) {
            if (header.key().equals("ce_specversion")) {
                assertEquals("1.0", header.value());
                foundSpecVersion = true;
            }
            if (header.key().equals("content-type")) {
                assertEquals("application/cloudevents", header.value());
                foundContentType = true;
            }
        }

        assertTrue(foundSpecVersion);
        assertTrue(foundContentType);
    }

    @Test
    void testS3Upload() {
        setUpS3();
        DocumentEvent mockEvent = mock(DocumentEvent.class);
        when(mockEvent.type()).thenReturn(DocumentEvent.Type.MUTATION);
        when(mockEvent.key()).thenReturn("test-key");
        when(mockEvent.bucket()).thenReturn("test-bucket");
        when(mockEvent.content())
                .thenReturn("{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}".getBytes());

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        handler.handle(params);

        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(mockS3Client).putObject(requestCaptor.capture(), bodyCaptor.capture());

        PutObjectRequest putObjectRequest = requestCaptor.getValue();
        assertEquals("test-bucket", putObjectRequest.bucket());
        assertTrue(putObjectRequest.key().startsWith("test-bucket/"));
        assertTrue(putObjectRequest.key().endsWith("test-key.json"));
    }
}
