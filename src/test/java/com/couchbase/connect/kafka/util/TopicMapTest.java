package com.couchbase.connect.kafka.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static org.junit.Assert.assertEquals;

public class TopicMapTest {
  @Test
  public void parseTopicToCollection() {
    assertEquals(
        mapOf(
            "topic1", new ScopeAndCollection("my-scope", "collection1"),
            "topic2", new ScopeAndCollection("their-scope", "collection2")
        ),
        TopicMap.parseTopicToCollection(listOf(
            "topic1=my-scope.collection1",
            "topic2=their-scope.collection2"
        ))
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

  @Test(expected = IllegalArgumentException.class)
  public void parseMissingDelim() {
    List<String> topicsToCollections = Arrays.asList(
        "topic1=myscope.collection1",
        "topic2.theirscope.collection2"
    );
    TopicMap.parseTopicToCollection(topicsToCollections);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseExtraDelim() {
    List<String> topicsToCollections = Arrays.asList(
        "topic1=myscope.collection1",
        "topic2==theirscope.collection2"
    );
    TopicMap.parseTopicToCollection(topicsToCollections);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseMalformedCollection() {
    TopicMap.parseTopicToCollection(listOf("foo=bar"));
  }

}
