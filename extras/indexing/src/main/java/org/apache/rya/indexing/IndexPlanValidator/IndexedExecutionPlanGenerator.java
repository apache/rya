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


import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.pcj.matching.QueryVariableNormalizer;

import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;

import com.google.common.collect.Lists;

public class IndexedExecutionPlanGenerator implements ExternalIndexMatcher {

    private final TupleExpr query;
    private final List<ExternalTupleSet> normalizedIndexList;

    public IndexedExecutionPlanGenerator(TupleExpr query, List<ExternalTupleSet> indexList) {
        this.query = query;
        final VarConstantIndexListPruner vci = new VarConstantIndexListPruner(query);
        normalizedIndexList = getNormalizedIndices(vci.getRelevantIndices(indexList));
    }

    public List<ExternalTupleSet> getNormalizedIndices() {
        return normalizedIndexList;
    }

    @Override
    public Iterator<TupleExpr> getIndexedTuples() {

        final ValidIndexCombinationGenerator vic = new ValidIndexCombinationGenerator(query);
        final Iterator<List<ExternalTupleSet>> iter = vic.getValidIndexCombos(normalizedIndexList);
        return new Iterator<TupleExpr>() {
            private TupleExpr next = null;
            private boolean hasNextCalled = false;
            private boolean isEmpty = false;

            @Override
            public boolean hasNext() {

                if (!hasNextCalled && !isEmpty) {
                    while (iter.hasNext()) {
                        final TupleExpr temp = GeneralizedExternalProcessor.process(query, iter.next());
                        if (temp != null) {
                            next = temp;
                            hasNextCalled = true;
                            return true;
                        }
                    }
                    isEmpty = true;
                    return false;
                } else if(isEmpty) {
                    return false;
                } else {
                    return true;
                }
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

    private List<ExternalTupleSet> getNormalizedIndices(List<ExternalTupleSet> indexSet) {

        ExternalTupleSet tempIndex;
        final List<ExternalTupleSet> normalizedIndexSet = Lists.newArrayList();

        for (final ExternalTupleSet e : indexSet) {
            List<TupleExpr> tupList = null;
            try {
                tupList = QueryVariableNormalizer.getNormalizedIndex(query, e.getTupleExpr());
            } catch (final Exception e1) {
                e1.printStackTrace();
            }

            for (final TupleExpr te : tupList) {
                tempIndex = (ExternalTupleSet) e.clone();
                tempIndex.setProjectionExpr((Projection) te);
                normalizedIndexSet.add(tempIndex);
            }
        }
        return normalizedIndexSet;
    }
}
