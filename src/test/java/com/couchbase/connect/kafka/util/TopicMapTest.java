package com.couchbase.connect.kafka.util;


import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TopicMapTest {
  @Test
  public void parseTopicToCollection() {
    assertEquals(
        mapOf(
            "topic1", new Keyspace("default-bucket", "my-scope", "collection1"),
            "topic2", new Keyspace("default-bucket", "their-scope", "collection2")
        ),
        TopicMap.parseTopicToCollection(listOf(
            "topic1=my-scope.collection1",
            "topic2=their-scope.collection2"
        ), "default-bucket")
    );
  }

  @Test
  public void parseTopicToCollectionWithDatabaseName() {
    assertEquals(
        mapOf(
            "topic1", new Keyspace("my-bucket", "my-scope", "collection1"),
            "topic2", new Keyspace("their-bucket", "their-scope", "collection2")
        ),
        TopicMap.parseTopicToCollection(listOf(
            "topic1=my-bucket.my-scope.collection1",
            "topic2=their-bucket.their-scope.collection2"
        ), "default-bucket")
    );
  }

  @Test
  public void parseCollectionToTopic() {
    assertEquals(
        mapOf(
            new ScopeAndCollection("my-scope", "collection1"), "topic1",
            new ScopeAndCollection("their-scope", "collection2"), "topic2"
        ),
        TopicMap.parseCollectionToTopic(listOf(
            "my-scope.collection1=topic1",
            "their-scope.collection2=topic2"
        ))
    );
  }

  @Test
  public void parseMissingDelim() {
    List<String> topicsToCollections = Arrays.asList(
        "topic1=myscope.collection1",
        "topic2.theirscope.collection2"
    );
    assertThrows(IllegalArgumentException.class, () -> TopicMap.parseTopicToCollection(topicsToCollections, "default-bucket"));
  }

  @Test
  public void parseExtraDelim() {
    List<String> topicsToCollections = Arrays.asList(
        "topic1=myscope.collection1",
        "topic2==theirscope.collection2"
    );
    assertThrows(IllegalArgumentException.class, () -> TopicMap.parseTopicToCollection(topicsToCollections, "default-bucket"));
  }

  @Test
  public void parseMalformedCollection() {
    assertThrows(IllegalArgumentException.class, () -> TopicMap.parseTopicToCollection(listOf("foo=bar"), "default-bucket"));
  }

}
