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


/**
 * A list of all the types of Geo indexers supported in Rya.
 */
public enum GeoIndexerType {
    /**
     * Geo Mesa based indexer.
     */
    GEO_MESA("org.apache.rya.indexing.accumulo.geo.GeoMesaGeoIndexer"),
    /**
     * Geo Wave based indexer.
     */
    GEO_WAVE("org.apache.rya.indexing.accumulo.geo.GeoWaveGeoIndexer"),
    /**
     * MongoDB based indexer.
     */
    MONGO_DB("org.apache.rya.indexing.mongodb.geo.MongoGeoIndexer"),
	/**
	 * No mention of a type is specified, so use default.
	 */
	UNSPECIFIED("no_index_was_configured");

	private String geoIndexerClassString;

    /**
     * Creates a new {@link GeoIndexerType}.
     * @param geoIndexerClass the {@link GeoIndexer} {@link Class}.
     * (not {@code null})
     */
    private GeoIndexerType(final String geoIndexerClassString) {
        this.geoIndexerClassString = checkNotNull(geoIndexerClassString);
    }

    /**
     * @return the {@link GeoIndexer} {@link Class}. (not {@code null})
     */
    public String getGeoIndexerClassString() {
    	
        return geoIndexerClassString;
	}

	/**
     * @return True if the class exists on the classpath.
     */
	public boolean isOnClassPath() {
		try {
			Class.forName(geoIndexerClassString, false, this.getClass().getClassLoader());
			return true;
		} catch (ClassNotFoundException e) {
			// it does not exist on the classpath
			return false;
		}
   	}
}
