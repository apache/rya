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
	 * initialize after setting configuration.
	 */
    @Override
    public void init();

    /**
     * Creates the {@link EventStorage} that will be used by the indexer.
     * NOTE: {@link #setConf(org.apache.hadoop.conf.Configuration)} must be called before calling this.
     * @return The {@link EventStorage} that will be used by this indexer.
     */
    public abstract EventStorage getEventStorage();

    /**
     * Used to indicate which geo filter functions to use in a query.
     */
    public static enum GeoPolicy {
        /**
         * The provided geo object equals the geo object where the event took place.
         */
        EQUALS(GeoConstants.GEO_SF_EQUALS),

        /**
         * The provided geo object does not share any space with the event.
         */
        DISJOINT(GeoConstants.GEO_SF_DISJOINT),

        /**
         * The provided geo object shares some amount of space with the event.
         */
        INTERSECTS(GeoConstants.GEO_SF_INTERSECTS),

        /**
         * The provided geo object shares a point with the event, but only on the edge.
         */
        TOUCHES(GeoConstants.GEO_SF_TOUCHES),

        /**
         * The provided geo object shares some, but not all space with the event.
         */
        CROSSES(GeoConstants.GEO_SF_CROSSES),

        /**
         * The provided geo object exists completely within the event.
         */
        WITHIN(GeoConstants.GEO_SF_WITHIN),

        /**
         * The event took place completely within the provided geo object.
         */
        CONTAINS(GeoConstants.GEO_SF_CONTAINS),

        /**
         * The provided geo object has some but not all points in common with the event,
         * are of the same dimension, and the intersection of the interiors has the
         * same dimension as the geometries themselves.
         */
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

    static final String TEMPORAL_NS = "tag:rya-rdf.org,2015:temporal#";
    /**
     * Used to indicate which temporal filter functions to use in a query.
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
         * The provided instant in time equals the start of the interval in which the event took place.
         */
        INSTANT_START_INTERVAL(false, new URIImpl(TEMPORAL_NS+"hasBeginningInterval")),

        /**
         * The provided instant in time equals the end of the interval in which the event took place.
         */
        INSTANT_END_INTERVAL(false, new URIImpl(TEMPORAL_NS+"hasEndInterval")),

        /**
         * The provided interval equals the interval in which the event took place.
         */
        INTERVAL_EQUALS(false, new URIImpl(TEMPORAL_NS+"intervalEquals")),

        /**
         * The provided interval is before the interval in which the event took place.
         */
        INTERVAL_BEFORE(false, new URIImpl(TEMPORAL_NS+"intervalBefore")),

        /**
         * The provided interval is after the interval in which the event took place.
         */
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
