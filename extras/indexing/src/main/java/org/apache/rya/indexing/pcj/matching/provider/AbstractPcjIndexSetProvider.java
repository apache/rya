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
package org.apache.rya.indexing.pcj.matching.provider;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.indexing.IndexPlanValidator.IndexedExecutionPlanGenerator;
import org.apache.rya.indexing.IndexPlanValidator.ValidIndexCombinationGenerator;
import org.apache.rya.indexing.external.matching.ExternalSetProvider;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.pcj.matching.PCJOptimizerUtilities;
import org.apache.rya.indexing.pcj.matching.PCJToSegmentConverter;
import org.openrdf.query.algebra.TupleExpr;

import com.google.common.annotations.VisibleForTesting;

/**
 * Abstraction of {@link ExternalSetProvider} that provides {@link ExternalTupleSet}s.
 * Implementations of this use either a user specified configuration information or user a specified
 * List of ExternalTupleSets to populate an internal cache of ExternalTupleSets.  If a configuration
 * is provided, the provider connects to an instance of RyaDetails and populates the cache with
 * PCJs registered in RyaDetails.
 */
public abstract class AbstractPcjIndexSetProvider implements ExternalSetProvider<ExternalTupleSet> {
    protected static final Logger log = Logger.getLogger(AbstractPcjIndexSetProvider.class);
    protected static final PCJToSegmentConverter converter = new PCJToSegmentConverter();
    protected List<ExternalTupleSet> indexCache;
    protected final Configuration conf;
    protected boolean init = false;

    /**
     * Creates a new {@link AbstractPcjIndexSetProvider} based on configuration only.
     * @param conf - The {@link Configuration} used to connect to {@link RyaDetails}.
     */
    public AbstractPcjIndexSetProvider(final Configuration conf) {
        this.conf = requireNonNull(conf);
    }

    /**
     * Creates a new {@link AbstractPcjIndexSetProvider} based user provided {@link ExternalTupleSet}s.
     * @param conf - The {@link Configuration} used to connect to {@link RyaDetails}.
     * @param indices - The {@link ExternalTupleSet}s to populate the internal cache.
     */
    public AbstractPcjIndexSetProvider(final Configuration conf, final List<ExternalTupleSet> indices) {
        requireNonNull(conf);
        this.conf = conf;
        indexCache = indices;
        init = true;
    }


    /**
     *
     * @param indices
     */
    @VisibleForTesting
    public void setIndices(final List<ExternalTupleSet> indices) {
        indexCache = indices;
        init = true;
    }

    /**
     * @param segment - QuerySegment used to get relevant queries form index cache for matching
     *
     * @return Iterator of Lists (combos) of PCJs used to build an optimal query plan
     */
    @Override
    public Iterator<List<ExternalTupleSet>> getExternalSetCombos(final QuerySegment<ExternalTupleSet> segment) {
        final ValidIndexCombinationGenerator comboGen = new ValidIndexCombinationGenerator(segment.getOrderedNodes());
        return comboGen.getValidIndexCombos(getExternalSets(segment));
    }

    /**
     * @param segment - QuerySegment used to get relevant queries form index cache for matching
     * @return List of PCJs for matching
     */
    @Override
    public List<ExternalTupleSet> getExternalSets(final QuerySegment<ExternalTupleSet> segment) {
        try {
            if(!init) {
                indexCache = PCJOptimizerUtilities.getValidPCJs(getIndices());
                init = true;
            }
            final TupleExpr query = segment.getQuery().getTupleExpr();
            final IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(query, indexCache);
            final List<ExternalTupleSet> pcjs = iep.getNormalizedIndices();
            final List<ExternalTupleSet> tuples = new ArrayList<>();
            for (final ExternalTupleSet tuple: pcjs) {
                final QuerySegment<ExternalTupleSet> pcj = converter.setToSegment(tuple);
                if (segment.containsQuerySegment(pcj)) {
                    tuples.add(tuple);
                }
            }
            return tuples;

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return The size of the set index cache.
     * @throws Exception
     */
    public int size() throws Exception {
        if (!init) {
            indexCache = PCJOptimizerUtilities.getValidPCJs(getIndices());
            init = true;
        }
        return indexCache.size();
    }

    /**
     * @param conf - client configuration
     * @return - list of {@link ExternalTupleSet}s or PCJs that are either
     *         specified by user in Configuration or exist in system.
     *
     * @throws Exception
     */
    protected abstract List<ExternalTupleSet> getIndices() throws PcjIndexSetException;

    /**
     * Exception thrown when failing to get the defined PCJS for a particular
     * index.
     */
    public class PcjIndexSetException extends Exception {
        public PcjIndexSetException(final String message) {
            super(message);
        }

        public PcjIndexSetException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
