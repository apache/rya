/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rya.indexing.accumulo.geo.GeoMesaGeoIndexer;
import org.apache.rya.indexing.accumulo.geo.GeoWaveGeoIndexer;
import org.apache.rya.indexing.mongodb.geo.MongoGeoIndexer;

/**
 * A list of all the types of Geo indexers supported in Rya.
 */
public enum GeoIndexerType {
    /**
     * Geo Mesa based indexer.
     */
    GEO_MESA(GeoMesaGeoIndexer.class),
    /**
     * Geo Wave based indexer.
     */
    GEO_WAVE(GeoWaveGeoIndexer.class),
    /**
     * MongoDB based indexer.
     */
    MONGO_DB(MongoGeoIndexer.class);

    private Class<? extends GeoIndexer> geoIndexerClass;

    /**
     * Creates a new {@link GeoIndexerType}.
     * @param geoIndexerClass the {@link GeoIndexer} {@link Class}.
     * (not {@code null})
     */
    private GeoIndexerType(final Class<? extends GeoIndexer> geoIndexerClass) {
        this.geoIndexerClass = checkNotNull(geoIndexerClass);
    }

    /**
     * @return the {@link GeoIndexer} {@link Class}. (not {@code null})
     */
    public Class<? extends GeoIndexer> getGeoIndexerClass() {
        return geoIndexerClass;
    }
}