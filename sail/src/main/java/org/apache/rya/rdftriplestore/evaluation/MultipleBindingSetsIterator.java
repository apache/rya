package org.apache.rya.rdftriplestore.evaluation;

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



import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.LookAheadIteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class MultipleBindingSetsIterator extends LookAheadIteration<BindingSet, QueryEvaluationException> {

    private final ParallelEvaluationStrategyImpl strategy;
    private final CloseableIteration leftIter;
    private StatementPattern stmtPtrn;
    private CloseableIteration<BindingSet, QueryEvaluationException> iter;
    //TODO: configurable
    private int batchSize = 1000;

    public MultipleBindingSetsIterator(ParallelEvaluationStrategyImpl strategy, TupleExpr leftArg, StatementPattern stmtPattern, BindingSet bindings)
            throws QueryEvaluationException {
        this.strategy = strategy;
        leftIter = strategy.evaluate(leftArg, bindings);
        this.stmtPtrn = stmtPattern;
        initIter();
    }

    public MultipleBindingSetsIterator(ParallelEvaluationStrategyImpl strategy, CloseableIteration leftIter, StatementPattern stmtPattern, BindingSet bindings)
            throws QueryEvaluationException {
        this.strategy = strategy;
        this.leftIter = leftIter;
        this.stmtPtrn = stmtPattern;
        initIter();
    }

    protected void initIter() throws QueryEvaluationException {
        try {
            Collection<BindingSet> sets = new ArrayList<BindingSet>();
            int i = 0;
            while (leftIter.hasNext()) {
                //default to 1K for the batch size
                if (i >= batchSize) {
                    break;
                }
                sets.add((BindingSet) leftIter.next());
                i++;
            }
            if (iter != null) iter.close();
            iter = strategy.evaluate(stmtPtrn, sets);
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    protected BindingSet getNextElement()
            throws QueryEvaluationException {
        try {
            while (true) {
                if (iter.hasNext()) {
                    return iter.next();
                }

                if (leftIter.hasNext()) {
                    initIter();
                } else
                    return null;
            }
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    protected void handleClose()
            throws QueryEvaluationException {
        try {
            super.handleClose();
            leftIter.close();
            iter.close();
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }
}
