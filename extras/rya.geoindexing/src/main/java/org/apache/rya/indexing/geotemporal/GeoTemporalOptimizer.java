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
package org.apache.rya.indexing.geotemporal;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.external.matching.AbstractExternalSetOptimizer;
import org.apache.rya.indexing.external.matching.ExternalSetMatcher;
import org.apache.rya.indexing.external.matching.ExternalSetProvider;
import org.apache.rya.indexing.external.matching.QueryNodeListRater;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.apache.rya.indexing.geotemporal.model.EventQueryNode;

import com.google.common.base.Optional;


public class GeoTemporalOptimizer extends AbstractExternalSetOptimizer<EventQueryNode> implements Configurable {
    private static final GeoTemporalExternalSetMatcherFactory MATCHER_FACTORY = new GeoTemporalExternalSetMatcherFactory();

    private GeoTemporalIndexer indexer;
    private GeoTemporalIndexSetProvider provider;
    private Configuration conf;

    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
        final GeoTemporalIndexerFactory factory = new GeoTemporalIndexerFactory();
        indexer = factory.getIndexer(conf);

        //conf here does not matter since EventStorage has already been set in the indexer.
        provider = new GeoTemporalIndexSetProvider(indexer.getEventStorage(conf));
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    protected ExternalSetMatcher<EventQueryNode> getMatcher(final QuerySegment<EventQueryNode> segment) {
        return MATCHER_FACTORY.getMatcher(segment);
    }

    @Override
    protected ExternalSetProvider<EventQueryNode> getProvider() {
        return provider;
    }

    @Override
    protected Optional<QueryNodeListRater> getNodeListRater(final QuerySegment<EventQueryNode> segment) {
        return null;
    }
}