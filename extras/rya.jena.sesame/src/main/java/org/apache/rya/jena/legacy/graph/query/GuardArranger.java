/*
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rya.jena.legacy.graph.query;

import java.util.Iterator;
import java.util.Set;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.CollectionFactory;

/**
 * A GuardArranger is initialized with a set of triple patterns, and can
 * then turn a set of constraints (plus a map giving the location of
 * variables) into an array of guards, where the i'th element of the
 * array is the set of constraints that can be evaluated as soon as
 * the i'th triple-pattern has been processed.
 */
public class GuardArranger {
    protected Set<String>[] boundVariables;
    protected int size;

    /**
     * Creates a new instance of {@link GuardArranger}.
     * @param triples the array of {@link Triple}s.
     */
    public GuardArranger(final Triple[] triples) {
        this.size = triples.length;
        this.boundVariables = makeBoundVariables(triples);
    }

    /**
     * @param triples the array of {@link Triple}s.
     * @return an array of sets exactly as long as the argument array of Triples.
     * The i'th element of the answer is the set of all variables that have been
     * matched when the i'th triple has been matched.
     */
    protected Set<String>[] makeBoundVariables(final Triple[] triples) {
        final int length = triples.length;
        @SuppressWarnings("unchecked")
        final Set<String>[] result = new Set[length];
        Set<String> prev = CollectionFactory.createHashedSet();
        for (int i = 0; i < length; i += 1) {
            prev = result[i] = Util.union(prev, Util.variablesOf(triples[i]));
        }
        return result;
    }

    /**
     * Creates an array of ExpressionSets exactly as long as the supplied length.
     * The i'th ExpressionSet contains the prepared [against {@code map}]
     * expressions that can be evaluated as soon as the i'th triple has been matched.
     * By "can be evaluated as soon as" we mean that all its variables are bound.
     * The original ExpressionSet is updated by removing those elements that can
     * be so evaluated.
     *
     * @param map the {@link Mapping} to prepare Expressions against.
     * @param constraints the set of constraint expressions to plant.
     * @return the array of prepared {@link ValuatorSet}s.
     */
    public ValuatorSet[] makeGuards(final Mapping map, final ExpressionSet constraints) {
        return makeGuards(map, constraints, size);
    }

    /**
     * Creates an array of ExpressionSets exactly as long as the supplied length.
     * The i'th ExpressionSet contains the prepared [against {@code map}]
     * expressions that can be evaluated as soon as the i'th triple has been matched.
     * By "can be evaluated as soon as" we mean that all its variables are bound.
     * The original ExpressionSet is updated by removing those elements that can
     * be so evaluated.
     *
     * @param map the {@link Mapping} to prepare Expressions against.
     * @param constraints the set of constraint expressions to plant.
     * @param length the number of evaluation slots available.
     * @return the array of prepared {@link ValuatorSet}s.
     */
    protected ValuatorSet[] makeGuards(final Mapping map, final ExpressionSet constraints, final int length) {
        final ValuatorSet[] result = new ValuatorSet[length];
        for (int i = 0; i < length; i += 1) {
            result[i] = new ValuatorSet();
        }
        final Iterator<Expression> it = constraints.iterator();
        while (it.hasNext()) {
            plantWhereFullyBound(it.next(), it, map, result);
        }
        return result;
    }

    /**
     * Find the earliest triple index where this expression can be evaluated, add it
     * to the appropriate expression set, and remove it from the original via the
     * iterator.
     * @param expression the {@link Expression}.
     * @param it the {@link Iterator} of {@link Expression}s.
     * @param map the {@link Mapping}.
     * @param valuatorSets the array of {@link ValuatorSet}s.
     */
    protected void plantWhereFullyBound(final Expression expression, final Iterator<Expression> it, final Mapping map, final ValuatorSet[] valuatorSets) {
        for (int i = 0; i < boundVariables.length; i += 1) {
            if (canEval(expression, i)) {
                valuatorSets[i].add(expression.prepare(map));
                it.remove();
                return;
            }
        }
    }

    /**
     * @param expression the {@link Expression}.
     * @param index the index.
     * @return {@code true} if this Expression can be evaluated after the
     * index'th triple has been matched, ie, all the variables of the expression
     * have been bound.
     */
    protected boolean canEval(final Expression expression, final int index) {
        return Expression.Util.containsAllVariablesOf(boundVariables[index], expression);
    }
}