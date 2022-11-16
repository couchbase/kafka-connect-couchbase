package com.couchbase.connect.kafka.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScopeAndCollectionTest {

  public String scope = "Ascope";
  public String collection = "Acollection";

  @Test
  public void parse() {
    ScopeAndCollection scopeAndCollection = ScopeAndCollection.parse(createFQCN(scope, collection));

    assertEquals(scope, scopeAndCollection.getScope());
    assertEquals(collection, scopeAndCollection.getCollection());
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseMissingDelim() throws Exception {
    ScopeAndCollection scopeAndCollection = ScopeAndCollection.parse(scope + collection);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseExtraDelim() throws Exception {
    ScopeAndCollection scopeAndCollection = ScopeAndCollection.parse(scope + ".." + collection);
  }

  @Test
  public void checkEqual() {
    ScopeAndCollection scopeAndCollection1 = ScopeAndCollection.parse(createFQCN(scope, collection));
    ScopeAndCollection scopeAndCollection2 = new ScopeAndCollection(scope, collection);
    assertTrue(scopeAndCollection1.equals(scopeAndCollection2));

  }

  public String createFQCN(String scope, String collection) {
    return scope + "." + collection;
  }
}
