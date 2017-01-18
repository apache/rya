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
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.geotemporal.mongo.MongoGeoTemporalIndexer;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoSecondaryIndex;

/**
 * Factory for retrieving a {@link GeoTemporalIndexer} based on a provided {@link Configuration}.
 */
public class GeoTemporalIndexerFactory {
    /**
     * Creates and returns a {@link GeoTemporalIndexer}.
     * @param conf - The {@link Configuration} to base the {@link GeoTemporalIndexer} on.
     * @return The created {@link GeoTemporalIndexer}.
     */
    public GeoTemporalIndexer getIndexer(final Configuration conf) {
        if(ConfigUtils.getUseMongo(conf)) {
            final MongoDBRdfConfiguration config = new MongoDBRdfConfiguration(conf);
            for(final MongoSecondaryIndex index : config.getAdditionalIndexers()) {
                if(index instanceof GeoTemporalIndexer) {
                    return (GeoTemporalIndexer) index;
                }
            }
            final MongoGeoTemporalIndexer index = new MongoGeoTemporalIndexer();
            index.setConf(conf);
            index.init();
            return index;
        } else {
            //TODO: add Accumulo here.
            return null;
        }
    }
}
