package com.netdocuments.connect.kafka.filter;

import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RegexKeyFilterTest {

    @Test
    void testInitWithValidRegex() {
        RegexKeyFilter filter = new RegexKeyFilter();
        Map<String, String> props = new HashMap<>();
        props.put("couchbase.event.filter.regex", "test.*");
        props.put("couchbase.event.filter.regex.case_insensitive", "true");

        assertDoesNotThrow(() -> filter.init(props));
    }

    @Test
    void testInitWithEmptyRegex() {
        RegexKeyFilter filter = new RegexKeyFilter();
        Map<String, String> props = new HashMap<>();
        props.put("couchbase.event.filter.regex", "");

        assertThrows(ConfigException.class, () -> filter.init(props));
    }

    @Test
    void testInitWithInvalidRegex() {
        RegexKeyFilter filter = new RegexKeyFilter();
        Map<String, String> props = new HashMap<>();
        props.put("couchbase.event.filter.regex", "[invalid");

        assertThrows(ConfigException.class, () -> filter.init(props));
    }

    @ParameterizedTest
    @ValueSource(strings = { "test123", "TEST456", "testABC" })
    void testPassWithMatchingKeysAndSharespaceFilterDisabled(String key) {
        RegexKeyFilter filter = new RegexKeyFilter();
        Map<String, String> props = new HashMap<>();
        props.put("couchbase.event.filter.regex", "test.*");
        props.put("couchbase.event.filter.regex.case_insensitive", "true");
        props.put("couchbase.event.filter.sharespace.filter", "false");
        filter.init(props);

        DocumentEvent event = mock(DocumentEvent.class);
        when(event.key()).thenReturn(key);

        assertTrue(filter.pass(event));
    }

    @ParameterizedTest
    @ValueSource(strings = { "abc123", "123test", "xyzTEST" })
    void testPassWithNonMatchingKeysAndSharespaceFilterDisabled(String key) {
        RegexKeyFilter filter = new RegexKeyFilter();
        Map<String, String> props = new HashMap<>();
        props.put("couchbase.event.filter.regex", "test.*");
        props.put("couchbase.event.filter.regex.case_insensitive", "true");
        props.put("couchbase.event.filter.sharespace.filter", "false");
        filter.init(props);

        DocumentEvent event = mock(DocumentEvent.class);
        when(event.key()).thenReturn(key);

        assertFalse(filter.pass(event));
    }

    @Test
    void testPassWithSharespaceFilterEnabled() {
        RegexKeyFilter filter = new RegexKeyFilter();
        Map<String, String> props = new HashMap<>();
        props.put("couchbase.event.filter.regex", ".*");
        props.put("couchbase.event.filter.regex.case_insensitive", "true");
        props.put("couchbase.event.filter.sharespace.filter", "true");
        filter.init(props);

        DocumentEvent shareSpaceEvent = mock(DocumentEvent.class);
        when(shareSpaceEvent.key()).thenReturn("MDucot5/q/b/t/i/^D240924115010829");
        assertFalse(filter.pass(shareSpaceEvent));

        DocumentEvent nonShareSpaceEvent = mock(DocumentEvent.class);
        when(nonShareSpaceEvent.key()).thenReturn("MDucot5/q/b/t/i/~240924115010829");
        assertTrue(filter.pass(nonShareSpaceEvent));
    }

    @Test
    void testPassWithCaseSensitiveRegexAndSharespaceFilter() {
        RegexKeyFilter filter = new RegexKeyFilter();
        Map<String, String> props = new HashMap<>();
        props.put("couchbase.event.filter.regex", ".*test.*");
        props.put("couchbase.event.filter.regex.case_insensitive", "false");
        props.put("couchbase.event.filter.sharespace.filter", "true");
        filter.init(props);

        DocumentEvent event1 = mock(DocumentEvent.class);
        when(event1.key()).thenReturn("MDucot5/q/b/t/i/~test123");
        assertTrue(filter.pass(event1));

        DocumentEvent event2 = mock(DocumentEvent.class);
        when(event2.key()).thenReturn("TEST123");
        assertFalse(filter.pass(event2));

        DocumentEvent shareSpaceEvent = mock(DocumentEvent.class);
        when(shareSpaceEvent.key()).thenReturn("MDucot5/q/b/t/i/^D240924115010829");
        assertFalse(filter.pass(shareSpaceEvent));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "MDucot5/qbti~240924115010829",
            "HDucot5/qbti~240924115010829",
            "HDucot5/qbti^f240924115010829",
            "HDucot5/qbti^w240924115010829",
            "HDucot5/qbti^c240924115010829",
            "HDucot5/qbti^b240924115010829",
            "HDucot5/qbti^r240924115010829"
    })
    void testIsShareSpaceWithInvalidKeys(String key) {
        assertFalse(RegexKeyFilter.isShareSpace(key), '"' + key + '"');
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "MDucot5/qbti^D240924115010829",
            "sharespacename",
            "something strange",
            "1",
            "MDucot5/qbtiNB240924115010829",
    })
    void testIsShareSpaceWithValidKeys(String key) {
        assertTrue(RegexKeyFilter.isShareSpace(key), '"' + key + '"');
    }

    @Test
    void testModifyKey() {
        String originalKey = "MDucot5/qbti~240924115010829";
        String expectedModifiedKey = "MDucot5/q/b/t/i/~240924115010829";
        assertEquals(expectedModifiedKey, RegexKeyFilter.modifyKey(originalKey));
    }

    @Test
    void testModifyKeyWithShortKey() {
        String shortKey = "short";
        assertEquals(shortKey, RegexKeyFilter.modifyKey(shortKey));
    }
}
