package org.apache.rya.indexing.external.matching;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * This class matches ExternalSet queries to sub-queries of a given
 * {@link OptionalJoinSegment}. A match will occur when the
 * {@link QueryModelNode}s of the ExternalSet can be grouped together in the
 * OptionalJoinSegment and ordered to match the ExternalSet query.
 *
 */

public class OptionalJoinSegmentMatcher<T extends ExternalSet> extends AbstractExternalSetMatcher<T> {

    private ExternalSetConverter<T> converter;

    public OptionalJoinSegmentMatcher(QuerySegment<T> segment, ExternalSetConverter<T> converter) {
        Preconditions.checkNotNull(segment);
        Preconditions.checkNotNull(converter);
        this.segment = segment;
        this.segmentNodeList = new ArrayList<>(segment.getOrderedNodes());
        this.converter = converter;
    }

    /**
     * @param segment
     *            - {@link QuerySegment} to be replaced by ExternalSet
     * @param set
     *            - ExternalSet to replace matching QuerySegment
     */
    @Override
    protected boolean match(QuerySegment<T> nodes, T set) {

        Preconditions.checkNotNull(nodes);
        Preconditions.checkNotNull(set);
        
        if (!segment.containsQuerySegment(nodes)) {
            return false;
        }
        final List<QueryModelNode> consolidatedNodes = groupNodesToMatchExternalSet(getOrderedNodes(),
                nodes.getOrderedNodes());
        if (consolidatedNodes.size() == 0) {
            return false;
        }

        // set segment nodes to the consolidated nodes to match pcj
        segment.setNodes(consolidatedNodes);
        final boolean nodesReplaced = segment.replaceWithExternalSet(nodes, set);

        // if ExternalSet nodes replaced queryNodes, update nodes
        // otherwise restore segment nodes back to original pre-consolidated
        // state
        if (nodesReplaced) {
            updateTupleAndNodes();
        } else {
            segment.setNodes(segmentNodeList);
        }

        return nodesReplaced;
    }

    /**
     *
     * @param queryNodes
     *            - query nodes to be compared to pcj for matching
     * @param pcjNodes
     *            - pcj nodes to match to query
     * @return - query nodes with pcj nodes grouped together (if possible),
     *         otherwise return an empty list.
     */
    private List<QueryModelNode> groupNodesToMatchExternalSet(final List<QueryModelNode> queryNodes,
            final List<QueryModelNode> externalSetNodes) {
        final QueryNodeConsolidator pnc = new QueryNodeConsolidator(queryNodes, externalSetNodes);
        final boolean canConsolidate = pnc.consolidateNodes();
        if (canConsolidate) {
            return pnc.getQueryNodes();
        }
        return new ArrayList<QueryModelNode>();
    }

    @Override
    public boolean match(T set) {
        Preconditions.checkNotNull(set);
        QuerySegment<T> segment = converter.setToSegment(set);
        return match(segment, set);
    }

    @Override
    public QuerySegment<T> match(Iterator<List<T>> sets, Optional<QueryNodeListRater> r) {
        QueryNodeListRater rater = null;

        if (r.isPresent()) {
            rater = r.get();
        } else {
            rater = new BasicRater(segmentNodeList);
        }

        double min= Double.MAX_VALUE;
        double temp = 0;
        QuerySegment<T> minSegment = null;
        while (sets.hasNext()) {
            QuerySegment<T> tempSegment = match(sets.next());
            temp = rater.rateQuerySegment(tempSegment.getOrderedNodes());
            if(temp < min) {
                min = temp;
                minSegment = tempSegment;
            }
        }

        return minSegment;
    }

    @Override
    public QuerySegment<T> match(Collection<T> sets) {
        Preconditions.checkNotNull(sets);
        QuerySegment<T> copy = ((OptionalJoinSegment<T>) segment).clone();
        for (T set : sets) {
            QuerySegment<T> nodes = converter.setToSegment(set);
            if (copy.containsQuerySegment(nodes)) {
                List<QueryModelNode> consolidatedNodes = groupNodesToMatchExternalSet(copy.getOrderedNodes(),
                        nodes.getOrderedNodes());
                if (consolidatedNodes.size() == 0) {
                    return copy;
                }

                // set segment nodes to the consolidated nodes to match
                copy.setNodes(consolidatedNodes);
                copy.replaceWithExternalSet(nodes, set);
            }
        }

        return copy;
    }


}
