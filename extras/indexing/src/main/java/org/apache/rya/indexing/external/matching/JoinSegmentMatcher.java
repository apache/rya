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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;

/**
 * This class is responsible for matching PCJ nodes with subsets of the
 * {@link QueryModelNode}s found in {@link JoinSegment}s. Each PCJ is reduced to
 * a bag of QueryModelNodes and set operations can be used to determine if the
 * PCJ is a subset of the JoinSegment. If it is a subset, the PCJ node replaces
 * the QueryModelNodes in the JoinSegment.
 *
 */

public class JoinSegmentMatcher<T extends ExternalSet> extends AbstractExternalSetMatcher<T> {

    private ExternalSetConverter<T> converter;

    public JoinSegmentMatcher(QuerySegment<T> segment, ExternalSetConverter<T> converter) {
        Preconditions.checkNotNull(segment);
        Preconditions.checkNotNull(converter);
        Preconditions.checkArgument(segment instanceof JoinSegment);
        this.segment = segment;
        this.segmentNodeList = new ArrayList<>(segment.getOrderedNodes());
        this.converter = converter;
    }

    /**
     * Match specified {@link QuerySegment} to this QuerySegment and insert
     * {@link ExternalSet} node T in place of matched nodes. Here, the specified
     * QuerySegment is derived from the ExternalSet node T.
     * 
     * @param segment
     *            - {@link QuerySegment} representation of node T
     * @param T
     *            - node to be matched
     */
    @Override
    protected boolean match(QuerySegment<T> nodes, T set) {
        Preconditions.checkArgument(nodes instanceof JoinSegment<?>);
        final boolean nodesReplaced = segment.replaceWithExternalSet(nodes, set);
        if (nodesReplaced) {
            updateTupleAndNodes();
        }

        return nodesReplaced;
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
        QuerySegment<T> copy = ((JoinSegment<T>) segment).clone();
        for (T set : sets) {
            QuerySegment<T> nodes = converter.setToSegment(set);
            copy.replaceWithExternalSet(nodes, set);
        }

        return copy;
    }


}
