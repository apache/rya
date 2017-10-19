package org.apache.rya.indexing.IndexPlanValidator;

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

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class TupleExecutionPlanGenerator implements IndexTupleGenerator {



    @Override
    public Iterator<TupleExpr> getPlans(final Iterator<TupleExpr> indexPlans) {

        final Iterator<TupleExpr> iter = indexPlans;

        return new Iterator<TupleExpr>() {

            private TupleExpr next = null;
            private boolean hasNextCalled = false;
            private boolean isEmpty = false;
            Iterator<TupleExpr> tuples = null;

            @Override
            public boolean hasNext() {

                if (!hasNextCalled && !isEmpty) {
                    if (tuples != null && tuples.hasNext()) {
                        next = tuples.next();
                        hasNextCalled = true;
                        return true;
                    } else {
                        while (iter.hasNext()) {
                            tuples = getPlans(iter.next()).iterator();
                            if (tuples == null) {
                                throw new IllegalStateException("Plans cannot be null!");
                            }
                            next = tuples.next();
                            hasNextCalled = true;
                            return true;
                        }
                        isEmpty = true;
                        return false;
                    }
                } else
                    return !isEmpty;
            }

            @Override
            public TupleExpr next() {

                if (hasNextCalled) {
                    hasNextCalled = false;
                    return next;
                } else if(isEmpty) {
                    throw new NoSuchElementException();
                }else {
                    if (this.hasNext()) {
                        hasNextCalled = false;
                        return next;
                    } else {
                        throw new NoSuchElementException();
                    }

                }

            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Cannot delete from iterator!");
            }

        };

    }

    private List<TupleExpr> getPlans(final TupleExpr te) {


        final NodeCollector nc = new NodeCollector();
        te.visit(nc);

        final Set<QueryModelNode> nodeSet = nc.getNodeSet();
        final List<Filter> filterList = nc.getFilterSet();
        final Projection projection = nc.getProjection().clone();

        final List<TupleExpr> queryPlans = Lists.newArrayList();

        final Collection<List<QueryModelNode>> plans = Collections2.permutations(nodeSet);

        for (final List<QueryModelNode> p : plans) {
            if (p.size() == 0) {
                throw new IllegalArgumentException("Tuple must contain at least one node!");
            } else if (p.size() == 1) {
                queryPlans.add(te);
            } else {
                queryPlans.add(buildTuple(p, filterList, projection));
            }
        }

        return queryPlans;
    }

    private TupleExpr buildTuple(final List<QueryModelNode> nodes, final List<Filter> filters, final Projection projection) {

        final Projection proj = projection.clone();
        Join join = null;

        join = new Join((TupleExpr) nodes.get(0).clone(), (TupleExpr) nodes.get(1).clone());

        for (int i = 2; i < nodes.size(); i++) {
            join = new Join(join, (TupleExpr) nodes.get(i).clone());
        }

        if (filters.size() == 0) {
            proj.setArg(join);
            return proj;
        } else {
            TupleExpr queryPlan = join;
            for (final Filter f : filters) {
                final Filter filt = f.clone();
                filt.setArg(queryPlan);
                queryPlan = filt;
            }
            proj.setArg(queryPlan);
            return proj;
        }

    }

    public static class NodeCollector extends AbstractQueryModelVisitor<RuntimeException> {

        private final Set<QueryModelNode> nodeSet = Sets.newHashSet();
        private final List<Filter> filterSet = Lists.newArrayList();
        private Projection projection;

        public Projection getProjection() {
            return projection;
        }

        public Set<QueryModelNode> getNodeSet() {
            return nodeSet;
        }

        public List<Filter> getFilterSet() {
            return filterSet;
        }

        @Override
        public void meet(final Projection node) {
            projection = node;
            node.getArg().visit(this);
        }

        @Override
        public void meetNode(final QueryModelNode node) throws RuntimeException {
            if (node instanceof ExternalTupleSet || node instanceof BindingSetAssignment
                    || node instanceof StatementPattern) {
                nodeSet.add(node);
            }
            super.meetNode(node);
        }

        @Override
        public void meet(final Filter node) {
            filterSet.add(node);
            node.getArg().visit(this);
        }

    }

}
