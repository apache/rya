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



import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStatistics;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;

import java.util.*;

/**
 * A query optimizer that re-orders nested Joins.
 *
 * @author Arjohn Kampman
 * @author James Leigh
 */
public class QueryJoinOptimizer implements QueryOptimizer {

    protected final EvaluationStatistics statistics;

    public QueryJoinOptimizer() {
        this(new EvaluationStatistics());
    }

    public QueryJoinOptimizer(EvaluationStatistics statistics) {
        this.statistics = statistics;
    }

    /**
     * Applies generally applicable optimizations: path expressions are sorted
     * from more to less specific.
     *
     * @param tupleExpr
     */
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(new JoinVisitor());
    }

    protected class JoinVisitor extends QueryModelVisitorBase<RuntimeException> {

        Set<String> boundVars = new HashSet<String>();

        @Override
        public void meet(LeftJoin leftJoin) {
            leftJoin.getLeftArg().visit(this);

            Set<String> origBoundVars = boundVars;
            try {
                boundVars = new HashSet<String>(boundVars);
                boundVars.addAll(leftJoin.getLeftArg().getBindingNames());

                leftJoin.getRightArg().visit(this);
            } finally {
                boundVars = origBoundVars;
            }
        }

        @Override
        public void meet(Join node) {
            Set<String> origBoundVars = boundVars;
            try {
                boundVars = new HashSet<String>(boundVars);

                // Recursively get the join arguments
                List<TupleExpr> joinArgs = getJoinArgs(node, new ArrayList<TupleExpr>());

                // Build maps of cardinalities and vars per tuple expression
                Map<TupleExpr, Double> cardinalityMap = new HashMap<TupleExpr, Double>();
//                Map<TupleExpr, List<Var>> varsMap = new HashMap<TupleExpr, List<Var>>();
//                Map<Var, Double> varCardinalityMap = new HashMap<Var, Double>();

                for (TupleExpr tupleExpr : joinArgs) {
                    double cardinality = statistics.getCardinality(tupleExpr);
//                    List<Var> statementPatternVars = getStatementPatternVars(tupleExpr);

                    cardinalityMap.put(tupleExpr, cardinality);
//                    varsMap.put(tupleExpr, statementPatternVars);
                }

                // Build map of var frequences
//                Map<Var, Integer> varFreqMap = new HashMap<Var, Integer>();
//                for (List<Var> varList : varsMap.values()) {
//                    getVarFreqMap(varList, varFreqMap);
//                }

                // Reorder the (recursive) join arguments to a more optimal sequence
                List<TupleExpr> orderedJoinArgs = new ArrayList<TupleExpr>(joinArgs.size());
                while (!joinArgs.isEmpty()) {
                    TupleExpr tupleExpr = selectNextTupleExpr(joinArgs, cardinalityMap
                    );
                    if (tupleExpr == null) {
                        break;
                    }

                    joinArgs.remove(tupleExpr);
                    orderedJoinArgs.add(tupleExpr);

                    // Recursively optimize join arguments
                    tupleExpr.visit(this);

                    boundVars.addAll(tupleExpr.getBindingNames());
                }

                // Build new join hierarchy
                // Note: generated hierarchy is right-recursive to help the
                // IterativeEvaluationOptimizer to factor out the left-most join
                // argument
                int i = 0;
                TupleExpr replacement = orderedJoinArgs.get(i);
                for (i++; i < orderedJoinArgs.size(); i++) {
                    replacement = new Join(replacement, orderedJoinArgs.get(i));
                }

                // Replace old join hierarchy
                node.replaceWith(replacement);
            } finally {
                boundVars = origBoundVars;
            }
        }

        protected <L extends List<TupleExpr>> L getJoinArgs(TupleExpr tupleExpr, L joinArgs) {
            if (tupleExpr instanceof Join) {
                Join join = (Join) tupleExpr;
                getJoinArgs(join.getLeftArg(), joinArgs);
                getJoinArgs(join.getRightArg(), joinArgs);
            } else {
                joinArgs.add(tupleExpr);
            }

            return joinArgs;
        }

        protected List<Var> getStatementPatternVars(TupleExpr tupleExpr) {
            List<StatementPattern> stPatterns = StatementPatternCollector.process(tupleExpr);
            List<Var> varList = new ArrayList<Var>(stPatterns.size() * 4);
            for (StatementPattern sp : stPatterns) {
                sp.getVars(varList);
            }
            return varList;
        }

        protected <M extends Map<Var, Integer>> M getVarFreqMap(List<Var> varList, M varFreqMap) {
            for (Var var : varList) {
                Integer freq = varFreqMap.get(var);
                freq = (freq == null) ? 1 : freq + 1;
                varFreqMap.put(var, freq);
            }
            return varFreqMap;
        }

        /**
         * Selects from a list of tuple expressions the next tuple expression that
         * should be evaluated. This method selects the tuple expression with
         * highest number of bound variables, preferring variables that have been
         * bound in other tuple expressions over variables with a fixed value.
         */
        protected TupleExpr selectNextTupleExpr(List<TupleExpr> expressions,
                                                Map<TupleExpr, Double> cardinalityMap
//                                                ,Map<TupleExpr, List<Var>> varsMap,
//                                                Map<Var, Integer> varFreqMap, Set<String> boundVars
        ) {
            double lowestCardinality = Double.MAX_VALUE;
            TupleExpr result = expressions.get(0);

            for (TupleExpr tupleExpr : expressions) {
                // Calculate a score for this tuple expression
//                double cardinality = getTupleExprCardinality(tupleExpr, cardinalityMap, varsMap, varFreqMap, boundVars);
                double cardinality = cardinalityMap.get(tupleExpr);
//                List<Var> vars = varsMap.get(tupleExpr);
//                List<Var> distinctUnboundVars = getUnboundVars(vars);
//                if (distinctUnboundVars.size() >= 2) {
//                    cardinality *= (distinctUnboundVars.size() + 1);
//                }

                if (cardinality < lowestCardinality) {
                    // More specific path expression found
                    lowestCardinality = cardinality;
                    result = tupleExpr;
                }
            }

            return result;
        }

        protected double getTupleExprCardinality(TupleExpr tupleExpr, Map<TupleExpr, Double> cardinalityMap,
                                                 Map<TupleExpr, List<Var>> varsMap, Map<Var, Integer> varFreqMap, Set<String> boundVars) {
            double cardinality = cardinalityMap.get(tupleExpr);

            List<Var> vars = varsMap.get(tupleExpr);

            // Compensate for variables that are bound earlier in the evaluation
            List<Var> unboundVars = getUnboundVars(vars);
            List<Var> constantVars = getConstantVars(vars);
            int nonConstantVarCount = vars.size() - constantVars.size();
            if (nonConstantVarCount > 0) {
                double exp = (double) unboundVars.size() / nonConstantVarCount;
                cardinality = Math.pow(cardinality, exp);
            }

            if (unboundVars.isEmpty()) {
                // Prefer patterns with more bound vars
                if (nonConstantVarCount > 0) {
                    cardinality /= nonConstantVarCount;
                }
            } else {
                // Prefer patterns that bind variables from other tuple expressions
                int foreignVarFreq = getForeignVarFreq(unboundVars, varFreqMap);
                if (foreignVarFreq > 0) {
                    cardinality /= foreignVarFreq;
                }
            }

            // Prefer patterns that bind more variables
            List<Var> distinctUnboundVars = getUnboundVars(new
                    HashSet<Var>(vars));
            if (distinctUnboundVars.size() >= 2) {
                cardinality /= distinctUnboundVars.size();
            }

            return cardinality;
        }

        protected List<Var> getConstantVars(Iterable<Var> vars) {
            List<Var> constantVars = new ArrayList<Var>();

            for (Var var : vars) {
                if (var.hasValue()) {
                    constantVars.add(var);
                }
            }

            return constantVars;
        }

        protected List<Var> getUnboundVars(Iterable<Var> vars) {
            List<Var> unboundVars = new ArrayList<Var>();

            for (Var var : vars) {
                if (!var.hasValue() && !this.boundVars.contains(var.getName())) {
                    unboundVars.add(var);
                }
            }

            return unboundVars;
        }

        protected int getForeignVarFreq(List<Var> ownUnboundVars, Map<Var, Integer> varFreqMap) {
            int result = 0;

            Map<Var, Integer> ownFreqMap = getVarFreqMap(ownUnboundVars, new HashMap<Var, Integer>());

            for (Map.Entry<Var, Integer> entry : ownFreqMap.entrySet()) {
                Var var = entry.getKey();
                int ownFreq = entry.getValue();
                result += varFreqMap.get(var) - ownFreq;
            }

            return result;
        }
    }
}
