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

import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.impl.EmptyBindingSet;

/**
 */
public class ParallelJoinIterator extends LookAheadIteration<BindingSet, QueryEvaluationException> {

    public static final EmptyBindingSet EMPTY_BINDING_SET = new EmptyBindingSet();

    private final EvaluationStrategy strategy;
    private final Join join;
    private final CloseableIteration<BindingSet, QueryEvaluationException> leftIter;

    private ExecutorService executorService;
    private Queue<ParallelIteratorWork> workQueue = new LinkedBlockingQueue<ParallelIteratorWork>();
    private ParallelIteratorWork currentWork;
    private int batch;

    public ParallelJoinIterator(EvaluationStrategy strategy, Join join, BindingSet bindings, ExecutorService executorService, int batch)
            throws QueryEvaluationException {
        this.strategy = strategy;
        this.join = join;
        leftIter = strategy.evaluate(join.getLeftArg(), bindings);

        this.executorService = executorService;
        this.batch = batch;
    }


    @Override
    protected BindingSet getNextElement() throws QueryEvaluationException {

        try {
            while (leftIter.hasNext() || !workQueue.isEmpty() || currentWork != null) {
                if (!workQueue.isEmpty() && currentWork == null) {
                    currentWork = workQueue.poll();
                }

                if (currentWork != null) {
                    BindingSet bindingSet = currentWork.queue.poll();
                    if (EMPTY_BINDING_SET.equals(bindingSet)) {
                        currentWork = null;
                        continue;
                    } else if (bindingSet == null) {
                        continue;
                    }
                    return bindingSet;
                }

                try {
                    for (int i = 0; i < batch; i++) {
                        if (leftIter.hasNext()) {
                            ParallelIteratorWork work = new ParallelIteratorWork((BindingSet) leftIter.next(), join.getRightArg());
                            workQueue.add(work);
                            executorService.execute(work);
                        } else
                            break;
                    }
                } catch (NoSuchElementException ignore) {
                }
            }
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
        return null;
    }

    @Override
    protected void handleClose() throws QueryEvaluationException {
        try {
            super.handleClose();
            leftIter.close();
//           rightIter.close();
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    private class ParallelIteratorWork implements Runnable {

        private BindingSet leftBindingSet;
        private TupleExpr rightTupleExpr;
        public LinkedBlockingQueue<BindingSet> queue = new LinkedBlockingQueue<BindingSet>();

        private ParallelIteratorWork(BindingSet leftBindingSet, TupleExpr rightTupleExpr) {
            this.leftBindingSet = leftBindingSet;
            this.rightTupleExpr = rightTupleExpr;
        }

        @Override
        public void run() {
            try {
                CloseableIteration<BindingSet, QueryEvaluationException> iter = strategy.evaluate(rightTupleExpr, leftBindingSet);
                while (iter.hasNext()) {
                    queue.add(iter.next());
                }
                queue.add(EMPTY_BINDING_SET);
                iter.close();
            } catch (QueryEvaluationException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
