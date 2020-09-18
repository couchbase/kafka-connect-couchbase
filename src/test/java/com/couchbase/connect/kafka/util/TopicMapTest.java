package com.couchbase.connect.kafka.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TopicMapTest {
  @Test
  public void parse(){
    List<String> topicsToCollections = Arrays.asList(
            "topic1=myscope.collection1",
            "topic2=theirscope.collection2"
    );
    Map map = TopicMap.parse(topicsToCollections);

    assertEquals(map.get("topic1"), new ScopeAndCollection("myscope", "collection1"));
    assertEquals(map.get("topic2"), new ScopeAndCollection("theirscope", "collection2"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseMissingDelim(){
    List<String> topicsToCollections = Arrays.asList(
            "topic1=myscope.collection1",
            "topic2.theirscope.collection2"
    );
    Map map = TopicMap.parse(topicsToCollections);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseExtraDelim(){
    List<String> topicsToCollections = Arrays.asList(
            "topic1=myscope.collection1",
            "topic2==theirscope.collection2"
    );
    Map map = TopicMap.parse(topicsToCollections);
  }

}
