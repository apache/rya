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

import java.util.*;

import org.apache.rya.indexing.external.matching.QueryNodesToTupleExpr.TupleExprAndNodes;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;

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

/**
 * This class provides implementations of methods common to all implementations
 * of the {@link ExternalSetMatcher} interface.
 *
 */
public abstract class AbstractExternalSetMatcher<T extends ExternalSet> implements ExternalSetMatcher<T> {

    protected QuerySegment<T> segment;
    protected List<QueryModelNode> segmentNodeList;
    protected TupleExpr tuple;
    protected Set<TupleExpr> unmatched;
    protected Set<Filter> filters;
    private final QuerySegmentFactory<T> factory = new QuerySegmentFactory<T>();

    /**
     * Matches {@link QuerySegment} with underlying QuerySegment. If match
     * occurs, corresponding nodes are replaced by ExternalSet node. After each
     * call of this method, call updateTuplesAndNodes to update related
     * information.
     *
     * @param nodes
     *            - QuerySegment representation of ExternalSet to be used for
     *            matching
     * @param set
     *            - {@link ExternalSet} used to replace matching ExternalSet
     *            nodes when match occurs
     * @return - true is match and replace occurs and false otherwise
     */
    protected abstract boolean match(QuerySegment<T> nodes, T set);

    /**
     * In following method, order is determined by the order in which the node
     * appear in the query.
     *
     * @return - an ordered view of the QueryModelNodes appearing tuple
     *
     */
    @Override
    public List<QueryModelNode> getOrderedNodes() {
        return Collections.unmodifiableList(segmentNodeList);
    }

    @Override
    public Set<Filter> getFilters() {
        return filters;
    }

    @Override
    public TupleExpr getQuery() {
        return tuple;
    }

    @Override
    public Set<TupleExpr> getUnmatchedArgNodes() {
        return unmatched;
    }

    @Override
    public QuerySegment<T> nodeToQuerySegment(final QueryModelNode node) {
        return factory.getQuerySegment(node);
    }

    @Override
    public List<QueryModelNode> getAllUnmatchedNodes() {
        final List<QueryModelNode> unmatched = new ArrayList<>();
        for (final QueryModelNode node : segmentNodeList) {
            if (!(node instanceof ExternalSet)) {
                unmatched.add(node);
            }
        }
        return unmatched;
    }

    protected void updateTupleAndNodes() {
        segmentNodeList = segment.getOrderedNodes();
        final TupleExprAndNodes tupAndNodes = segment.getQuery();
        tuple = tupAndNodes.getTupleExpr();
        filters = tupAndNodes.getFilters();
        unmatched = new HashSet<>();
        final List<QueryModelNode> nodes = tupAndNodes.getNodes();
        for (final QueryModelNode q : nodes) {
            if (q instanceof UnaryTupleOperator || q instanceof BinaryTupleOperator) {
                unmatched.add((TupleExpr) q);
            } else if (q instanceof FlattenedOptional) {
                final FlattenedOptional opt = (FlattenedOptional) q;
                final TupleExpr rightArg = opt.getRightArg();
                if (rightArg instanceof UnaryTupleOperator || rightArg instanceof BinaryTupleOperator) {
                    unmatched.add(rightArg);
                }
            }
        }
    }

}
