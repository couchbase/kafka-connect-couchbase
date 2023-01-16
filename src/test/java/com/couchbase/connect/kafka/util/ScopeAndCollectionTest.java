package com.couchbase.connect.kafka.util;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ScopeAndCollectionTest {

  public String scope = "Ascope";
  public String collection = "Acollection";

  @Test
  public void parse() {
    ScopeAndCollection scopeAndCollection = ScopeAndCollection.parse(createFQCN(scope, collection));

    assertEquals(scope, scopeAndCollection.getScope());
    assertEquals(collection, scopeAndCollection.getCollection());
  }

  @Test
  public void parseMissingDelim() {
    assertThrows(IllegalArgumentException.class, () -> ScopeAndCollection.parse(scope + collection));
  }

  @Test
  public void parseExtraDelim() {
    assertThrows(IllegalArgumentException.class, () -> ScopeAndCollection.parse(scope + ".." + collection));
  }

  @Test
  public void checkEqual() {
    ScopeAndCollection scopeAndCollection1 = ScopeAndCollection.parse(createFQCN(scope, collection));
    ScopeAndCollection scopeAndCollection2 = new ScopeAndCollection(scope, collection);
    assertEquals(scopeAndCollection1, scopeAndCollection2);
  }

  public String createFQCN(String scope, String collection) {
    return scope + "." + collection;
  }
}
