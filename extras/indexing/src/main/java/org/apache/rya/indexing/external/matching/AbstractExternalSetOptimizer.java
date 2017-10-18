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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Optional;
import org.apache.rya.indexing.external.matching.QueryNodesToTupleExpr.TupleExprAndNodes;
import org.apache.rya.indexing.pcj.matching.PCJOptimizerUtilities;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;
import org.eclipse.rdf4j.query.algebra.helpers.QueryModelVisitorBase;

/**
 * Abstract base class meant to be extended by any QueryOptimizer that matches ExternalSets
 * to QueryModelNodes within the parsed query plan. 
 *
 * @param <T> - ExternalSet parameter
 */
public abstract class AbstractExternalSetOptimizer<T extends ExternalSet> implements QueryOptimizer {

    protected boolean useOptimal = false;
    
    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        QuerySegmentMatchVisitor visitor = new QuerySegmentMatchVisitor();
        tupleExpr.visit(visitor);
    }

    /**
     * This visitor navigates query until it reaches either a Join, Filter, or
     * LeftJoin. Once it reaches this node, it creates the appropriate
     * ExternalSetMatcher and uses this to match each of the {@link ExternalSet}
     * s to the {@link QuerySegment} starting with the Join, Filter, or
     * LeftJoin. Once each ExternalSet has been compared for matching, the
     * portion of the query starting with the Join, Filter, or LeftJoin is
     * replaced by the {@link TupleExpr} returned by
     * {@link ExternalSetMatcher#getQuery()}. This visitor then visits each of
     * the nodes returned by {@link ExternalSetMatcher#getUnmatchedArgs()}.
     *
     */
    protected class QuerySegmentMatchVisitor extends QueryModelVisitorBase<RuntimeException> {

        private final QuerySegmentFactory<T> factory = new QuerySegmentFactory<T>();

        @Override
        public void meetNode(QueryModelNode node) {

            if (checkNode(node)) {
                QuerySegment<T> segment = factory.getQuerySegment(node);
                ExternalSetProvider<T> provider = getProvider();
                ExternalSetMatcher<T> matcher = getMatcher(segment);
                QuerySegment<T> tempSeg = null;
                if(useOptimal) {
                    tempSeg = matcher.match(provider.getExternalSetCombos(segment), getNodeListRater(segment));
                } else {
                    tempSeg = matcher.match(provider.getExternalSets(segment));
                }
                
                TupleExprAndNodes tups = tempSeg.getQuery();
                node.replaceWith(tups.getTupleExpr());
                Set<TupleExpr> unmatched = getUnMatchedArgNodes(tups.getNodes());
                PCJOptimizerUtilities.relocateFilters(tups.getFilters());

                for (final TupleExpr tupleExpr : unmatched) {
                    tupleExpr.visit(this);
                }
            } else {
                super.meetNode(node);
            }
        }
    }

    private Set<TupleExpr> getUnMatchedArgNodes(List<QueryModelNode> nodes) {
        Set<TupleExpr> unmatched = new HashSet<>();
        for (final QueryModelNode q : nodes) {
            if (q instanceof UnaryTupleOperator || q instanceof BinaryTupleOperator) {
                unmatched.add((TupleExpr) q);
            } else if (q instanceof FlattenedOptional) {
                FlattenedOptional opt = (FlattenedOptional) q;
                TupleExpr rightArg = opt.getRightArg();
                if (rightArg instanceof UnaryTupleOperator || rightArg instanceof BinaryTupleOperator) {
                    unmatched.add(rightArg);
                }
            }
        }
        return unmatched;
    }
    
    
    private static boolean checkNode(QueryModelNode node) {
        return (node instanceof Join || node instanceof Filter || node instanceof LeftJoin);
    }

    /**
     * Get Matcher used to match ExternalSets to query
     * 
     * @param segment
     * @return
     */
    protected abstract ExternalSetMatcher<T> getMatcher(QuerySegment<T> segment);

    /**
     * Get ExternalSetProvider for source of ExternalSets to match to query
     * 
     * @return
     */
    protected abstract ExternalSetProvider<T> getProvider();
    
    /**
     * Get QueryNodeListRater to find optimal QueryNodeList after matching ExternalSets
     * 
     * @return 
     */
    protected abstract Optional<QueryNodeListRater> getNodeListRater(QuerySegment<T> segment);

}
