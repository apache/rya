/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.geotemporal;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.persist.index.RyaSecondaryIndexer;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

/**
 * A repository to store, index, and retrieve {@link Statement}s based on geotemporal features.
 */
public interface GeoTemporalIndexer extends RyaSecondaryIndexer {

    /**
     * Creates the {@link Eventtorage} that will be used by the indexer.
     *
     * @param conf - Indicates how the {@link EventStorage} is initialized. (not null)
     * @return The {@link EventStorage} that will be used by this indexer.
     */
    public abstract EventStorage getEventStorage(final Configuration conf);

    public enum GeoPolicy {
        EQUALS(GeoConstants.GEO_SF_EQUALS),
        DISJOINT(GeoConstants.GEO_SF_DISJOINT),
        INTERSECTS(GeoConstants.GEO_SF_INTERSECTS),
        TOUCHES(GeoConstants.GEO_SF_TOUCHES),
        CROSSES(GeoConstants.GEO_SF_CROSSES),
        WITHIN(GeoConstants.GEO_SF_WITHIN),
        CONTAINS(GeoConstants.GEO_SF_CONTAINS),
        OVERLAPS(GeoConstants.GEO_SF_OVERLAPS);

        private final URI uri;

        private GeoPolicy(final URI uri) {
            this.uri = uri;
        }

        public URI getURI() {
            return uri;
        }

        public static GeoPolicy fromURI(final URI uri) {
            for(final GeoPolicy policy : GeoPolicy.values()) {
                if(policy.getURI().equals(uri)) {
                    return policy;
                }
            }
            return null;
        }
    }

    String TEMPORAL_NS = "tag:rya-rdf.org,2015:temporal#";
    /**
     * All of the filter functions that can be used in a temporal based query.
     * <p>
     */
    public enum TemporalPolicy {
        /**
         * The provided instant in time equals the instant the event took place.
         */
        INSTANT_EQUALS_INSTANT(true, new URIImpl(TEMPORAL_NS+"equals")),

        /**
         * The provided instant in time was before when the event took place.
         */
        INSTANT_BEFORE_INSTANT(true, new URIImpl(TEMPORAL_NS+"before")),

        /**
         * The provided instant in time was after when the event took place.
         */
        INSTANT_AFTER_INSTANT(true, new URIImpl(TEMPORAL_NS+"after")),

        /**
         * The provided instant in time was before a time period.
         */
        INSTANT_BEFORE_INTERVAL(false, new URIImpl(TEMPORAL_NS+"beforeInterval")),

        /**
         * The provided instant in time took place within a set of time.
         */
        INSTANT_IN_INTERVAL(false, new URIImpl(TEMPORAL_NS+"insideInterval")),

        /**
         * The provided instant in time took place after a time period.
         */
        INSTANT_AFTER_INTERVAL(false, new URIImpl(TEMPORAL_NS+"afterInterval")),

        /**
         * The provided instant in time equals the instant the event took place.
         */
        INSTANT_START_INTERVAL(false, new URIImpl(TEMPORAL_NS+"hasBeginningInterval")),
        INSTANT_END_INTERVAL(false, new URIImpl(TEMPORAL_NS+"hasEndInterval")),
        INTERVAL_EQUALS(false, new URIImpl(TEMPORAL_NS+"intervalEquals")),
        INTERVAL_BEFORE(false, new URIImpl(TEMPORAL_NS+"intervalBefore")),
        INTERVAL_AFTER(false, new URIImpl(TEMPORAL_NS+"intervalAfter"));

        private final boolean isInstant;
        private final URI uri;

        TemporalPolicy(final boolean isInstant, final URI uri) {
            this.isInstant = isInstant;
            this.uri = uri;
        }

        public boolean isInstant(){
            return isInstant;
        }

        public URI getURI() {
            return uri;
        }

        public static TemporalPolicy fromURI(final URI uri) {
            for(final TemporalPolicy policy : TemporalPolicy.values()) {
                if(policy.getURI().equals(uri)) {
                    return policy;
                }
            }
            return null;
        }
    }
}
