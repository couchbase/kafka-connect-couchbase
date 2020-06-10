/*
 * Copyright 2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka.handler.source;

import com.couchbase.client.dcp.highlevel.Deletion;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.client.dcp.highlevel.Mutation;

import java.util.Locale;
import java.util.Optional;

import static com.couchbase.connect.kafka.handler.source.DocumentEvent.Type.DELETION;
import static com.couchbase.connect.kafka.handler.source.DocumentEvent.Type.EXPIRATION;
import static com.couchbase.connect.kafka.handler.source.DocumentEvent.Type.UNKNOWN;
import static java.util.Objects.requireNonNull;

/**
 * A Couchbase document change event.
 */
public class DocumentEvent {

  public enum Type {
    MUTATION,
    DELETION,
    EXPIRATION,
    UNKNOWN;

    private final String schemaName;

    Type() {
      this.schemaName = name().toLowerCase(Locale.ROOT);
    }

    public String schemaName() {
      return schemaName;
    }
  }

  private final DocumentChange change;
  private final String bucket;

  public static DocumentEvent create(DocumentChange change, String bucket) {
    return new DocumentEvent(change, bucket);
  }

  private DocumentEvent(DocumentChange change, String bucket) {
    this.change = requireNonNull(change);
    this.bucket = requireNonNull(bucket);
  }

  /**
   * Returns the content of the changed document.
   * <p>
   * For deletions and expirations, this is an empty byte array.
   */
  public byte[] content() {
    return change.getContent();
  }

  public String bucket() {
    return bucket;
  }

  /**
   * Returns the partition (also known as "vbucket")
   */
  public short partition() {
    return (short) change.getVbucket();
  }

  /**
   * Returns the partition UUID (also known as "vbucket UUID")
   */
  public long partitionUuid() {
    return change.getOffset().getVbuuid();
  }

  /**
   * Returns the document ID.
   */
  public String key() {
    return change.getKey();
  }

  /**
   * Returns the document ID prefixed by scope and collection.
   */
  public String qualifiedKey() {
    return change.getQualifiedKey();
  }

  public long cas() {
    return change.getCas();
  }

  public long bySeqno() {
    return change.getOffset().getSeqno();
  }

  public long revisionSeqno() {
    return change.getRevision();
  }

  /**
   * Returns true if the document was created or updated,
   * otherwise false.
   */
  public boolean isMutation() {
    return change instanceof Mutation;
  }

  /**
   * Returns additional metadata if this is a mutation event,
   * otherwise returns an empty optional.
   */
  public Optional<MutationMetadata> mutationMetadata() {
    return isMutation()
        ? Optional.of(new MutationMetadata((Mutation) change))
        : Optional.empty();
  }

  /**
   * Returns information about the scope and collection associated with this event.
   */
  public CollectionMetadata collectionMetadata() {
    return new CollectionMetadata(change);
  }

  public Type type() {
    if (change instanceof Mutation) {
      return Type.MUTATION;
    } else if (change instanceof Deletion) {
      return ((Deletion) change).isDueToExpiration() ? EXPIRATION : DELETION;
    }
    return UNKNOWN;
  }
}
