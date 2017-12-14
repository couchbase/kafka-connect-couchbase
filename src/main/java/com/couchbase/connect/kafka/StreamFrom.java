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

package com.couchbase.connect.kafka;

/**
 * The resume modes supported by the Kafka connector.
 */
public enum StreamFrom {
    SAVED_OFFSET_OR_BEGINNING,
    SAVED_OFFSET_OR_NOW,
    BEGINNING,
    NOW;

    /**
     * Returns the "fallback" mode for modes that prefer saved offsets, otherwise returns {@code this}.
     */
    public StreamFrom withoutSavedOffset() {
        switch (this) {
            case SAVED_OFFSET_OR_BEGINNING:
            case BEGINNING:
                return BEGINNING;

            case SAVED_OFFSET_OR_NOW:
            case NOW:
                return NOW;

            default:
                throw new AssertionError();
        }
    }

    /**
     * Returns true if this mode prefers to use saved offset.
     */
    public boolean isSavedOffset() {
        return this != withoutSavedOffset();
    }

    public com.couchbase.client.dcp.StreamFrom asDcpStreamFrom() {
        switch (this) {
            case BEGINNING:
                return com.couchbase.client.dcp.StreamFrom.BEGINNING;
            case NOW:
                return com.couchbase.client.dcp.StreamFrom.NOW;
            default:
                throw new IllegalStateException(this + " has no DCP counterpart");
        }
    }
}
