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

import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.GeoMesaGeoIndexer;
import org.apache.rya.indexing.geotemporal.GeoTemporalOptimizer;
import org.apache.rya.indexing.geotemporal.mongo.MongoGeoTemporalIndexer;
import org.apache.rya.indexing.mongodb.geo.MongoGeoIndexer;
import org.openrdf.model.URI;

import com.google.common.collect.Lists;

/**
 * A set of configuration utils to read a Hadoop {@link Configuration} object and create Cloudbase/Accumulo objects.
 * Soon will deprecate this class.  Use installer for the set methods, use {@link RyaDetails} for the get methods.
 * New code must separate parameters that are set at Rya install time from that which is specific to the client.
 * Also Accumulo index tables are pushed down to the implementation and not configured in conf.
 */
public class OptionalConfigUtils extends ConfigUtils {
    private static final Logger logger = Logger.getLogger(OptionalConfigUtils.class);


    public static final String GEO_NUM_PARTITIONS = "sc.geo.numPartitions";

    public static final String USE_GEO = "sc.use_geo";
    public static final String USE_GEOTEMPORAL = "sc.use_geotemporal";
    public static final String USE_FREETEXT = "sc.use_freetext";
    public static final String USE_TEMPORAL = "sc.use_temporal";
    public static final String USE_ENTITY = "sc.use_entity";
    public static final String USE_PCJ = "sc.use_pcj";
    public static final String USE_OPTIMAL_PCJ = "sc.use.optimal.pcj";
    public static final String USE_PCJ_UPDATER_INDEX = "sc.use.updater";
    public static final String GEO_PREDICATES_LIST = "sc.geo.predicates";
    public static final String GEO_INDEXER_TYPE = "sc.geo.geo_indexer_type";

    public static Set<URI> getGeoPredicates(final Configuration conf) {
        return getPredicates(conf, GEO_PREDICATES_LIST);
    }

    public static int getGeoNumPartitions(final Configuration conf) {
        return conf.getInt(GEO_NUM_PARTITIONS, getNumPartitions(conf));
    }

    public static boolean getUseGeo(final Configuration conf) {
        return conf.getBoolean(USE_GEO, false);
    }

    public static boolean getUseGeoTemporal(final Configuration conf) {
        return conf.getBoolean(USE_GEOTEMPORAL, false);
    }

    /**
     * Retrieves the value for the geo indexer type from the config.
     * @param conf the {@link Configuration}.
     * @return the {@link GeoIndexerType} found in the config or
     * {@code null} if it doesn't exist.
     */
    public static GeoIndexerType getGeoIndexerType(final Configuration conf) {
        return conf.getEnum(GEO_INDEXER_TYPE, null);
    }

    public static void setIndexers(final RdfCloudTripleStoreConfiguration conf) {
        final List<String> indexList = Lists.newArrayList();
        final List<String> optimizers = Lists.newArrayList();

        boolean useFilterIndex = false;
        ConfigUtils.setIndexers(conf);
        final String[] existingIndexers = conf.getStrings(AccumuloRdfConfiguration.CONF_ADDITIONAL_INDEXERS);
        if(existingIndexers != null ) {
            for (final String index : existingIndexers) {
                indexList.add(index);
            }
            for (final String optimizer : conf.getStrings(RdfCloudTripleStoreConfiguration.CONF_OPTIMIZERS)){
                optimizers.add(optimizer);
            }
        }

        final GeoIndexerType geoIndexerType = getGeoIndexerType(conf);

        if (ConfigUtils.getUseMongo(conf)) {
            if (getUseGeo(conf)) {
                if (geoIndexerType == null) {
                    // Default to MongoGeoIndexer if not specified
                    indexList.add(MongoGeoIndexer.class.getName());
                } else {
                    indexList.add(geoIndexerType.getGeoIndexerClass().getName());
                }
                useFilterIndex = true;
            }

            if (getUseGeoTemporal(conf)) {
                indexList.add(MongoGeoTemporalIndexer.class.getName());
                optimizers.add(GeoTemporalOptimizer.class.getName());
            }
        } else {
            if (getUseGeo(conf)) {
                if (geoIndexerType == null) {
                    // Default to GeoMesaGeoIndexer if not specified
                    indexList.add(GeoMesaGeoIndexer.class.getName());
                } else {
                    indexList.add(geoIndexerType.getGeoIndexerClass().getName());
                }
                useFilterIndex = true;
            }
        }

        if (useFilterIndex) {
            optimizers.remove(FilterFunctionOptimizer.class.getName());
            optimizers.add(GeoEnabledFilterFunctionOptimizer.class.getName());
        }

        conf.setStrings(AccumuloRdfConfiguration.CONF_ADDITIONAL_INDEXERS, indexList.toArray(new String[]{}));
        conf.setStrings(RdfCloudTripleStoreConfiguration.CONF_OPTIMIZERS, optimizers.toArray(new String[]{}));
    }
}
