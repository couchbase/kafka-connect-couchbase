package com.couchbase.connect.kafka.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

/**
 * Unit tests for the JsonPropertyExtractor class.
 * These tests cover various scenarios to ensure the correct functionality
 * of the JSON property extraction process.
 */
class JsonPropertyExtractorTests {

    /**
     * Test extraction of simple properties from a flat JSON object.
     * This test covers extraction of string, integer, double, and boolean values.
     */
    @Test
    void testSimpleJson() throws Exception {
        String json = "{\"name\":\"John\",\"age\":30,\"height\":1.75,\"isStudent\":false}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("name", "age", "height", "isStudent"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertEquals(4, result.size());
        assertEquals("John", result.get("name"));
        assertEquals(30, result.get("age"));
        assertEquals(1.75, result.get("height"));
        assertEquals(false, result.get("isStudent"));
    }

    /**
     * Test extraction of properties from nested JSON objects.
     * This test verifies that the extractor can handle dot notation for nested
     * properties.
     */
    @Test
    void testNestedJson() throws Exception {
        String json = "{\"person\":{\"name\":\"Alice\",\"age\":25,\"isEmployee\":true},\"city\":\"New York\"}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("person.name", "person.isEmployee", "city"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertEquals(3, result.size());
        assertEquals("Alice", result.get("person.name"));
        assertEquals(true, result.get("person.isEmployee"));
        assertEquals("New York", result.get("city"));
    }

    /**
     * Test extraction of properties from JSON arrays.
     * This test checks if the extractor can handle array indexing and mixed-type
     * arrays.
     */
    @Test
    void testArrayJson() throws Exception {
        String json = "{\"numbers\":[1,2,3],\"flags\":[true,false,true],\"mixed\":[1,\"two\",3.14,false]}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("numbers[1]", "flags[2]", "mixed[2]"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertEquals(3, result.size());
        assertEquals(2, result.get("numbers[1]"));
        assertEquals(true, result.get("flags[2]"));
        assertEquals(3.14, result.get("mixed[2]"));
    }

    /**
     * Test extraction of properties from a complex JSON structure.
     * This test verifies handling of nested objects, arrays, and various data
     * types.
     */
    @Test
    void testComplexJson() throws Exception {
        String json = "{\"person\":{\"name\":\"Bob\",\"age\":40,\"address\":{\"city\":\"London\",\"zipCode\":12345}},\"scores\":[9.5,8.0,9.0]}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("person.name", "person.address", "scores[1]"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertEquals(3, result.size());
        assertEquals("Bob", result.get("person.name"));
        assertEquals(8.0, result.get("scores[1]"));

        @SuppressWarnings("unchecked")
        Map<String, Object> address = (Map<String, Object>) result.get("person.address");
        assertNotNull(address);
        assertEquals(2, address.size());
        assertEquals("London", address.get("city"));
        assertEquals(12345, address.get("zipCode"));
    }

    /**
     * Test extraction from an empty JSON object.
     * This test ensures that the extractor handles empty JSON gracefully.
     */
    @Test
    void testEmptyJson() throws Exception {
        String json = "{}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("name", "age"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertTrue(result.isEmpty());
    }

    /**
     * Test extraction of non-existent properties.
     * This test verifies that the extractor correctly handles requests for
     * properties that don't exist in the JSON.
     */
    @Test
    void testNonExistentProperties() throws Exception {
        String json = "{\"name\":\"John\",\"age\":30}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("email", "phone"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertTrue(result.isEmpty());
    }

    /**
     * Test extraction of null values.
     * This test ensures that the extractor correctly handles null values in the
     * JSON.
     */
    @Test
    void testNullValues() throws Exception {
        String json = "{\"name\":\"John\",\"age\":null,\"address\":null}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("name", "age", "address"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertEquals(3, result.size());
        assertEquals("John", result.get("name"));
        assertNull(result.get("age"));
        assertNull(result.get("address"));
    }
}
