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
package org.apache.rya.api.function.Filter;

import static java.util.Objects.requireNonNull;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import info.aduna.iteration.CloseableIteration;

/**
 * Processes a {@link Filter} node from a SPARQL query.
 */
@DefaultAnnotation(NonNull.class)
public class FilterEvaluator {
    private static final Logger log = LoggerFactory.getLogger(FilterEvaluator.class);

    /**
     * Is used to evaluate the conditions of a {@link Filter}.
     */
    private static final EvaluationStrategyImpl EVALUATOR = new EvaluationStrategyImpl(
            new TripleSource() {
                private final ValueFactory valueFactory = new ValueFactoryImpl();

                @Override
                public ValueFactory getValueFactory() {
                    return valueFactory;
                }

                @Override
                public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
                        final Resource arg0,
                        final URI arg1,
                        final Value arg2,
                        final Resource... arg3) throws QueryEvaluationException {
                    throw new UnsupportedOperationException();
                }
            });

    private final ValueExpr condition;

    /**
     * Constructs an instance of {@link FilterEvaluator}.
     *
     * @param condition - The condition that defines what passes the filter function. (not null)
     */
    public FilterEvaluator(final ValueExpr condition) {
        this.condition = requireNonNull(condition);
    }

    /**
     * Make a {@link FilterEvaluator} that processes the logic of a {@link Filter}.
     *
     * @param filter - Defines the Filter that will be processed. (not null)
     * @return The {@link FilterEvaluator} for the provided {@link Filter}.
     */
    public static FilterEvaluator make(final Filter filter) {
        requireNonNull(filter);
        final ValueExpr condition = filter.getCondition();
        return new FilterEvaluator(condition);
    }

    /**
     * Checks to see if a {@link VisibilityBindingSet} should be included in the results or not.
     *
     * @param bs - The value that will be evaluated against the filter. (not null)
     * @return {@code true} if the binding set matches the filter and it should be included in the node's results,
     *   otherwise {@code false} and it should be excluded.
     */
    public boolean filter(final VisibilityBindingSet bs) {
        requireNonNull(bs);

        try {
            final Value result = EVALUATOR.evaluate(condition, bs);
            return QueryEvaluationUtil.getEffectiveBooleanValue(result);
        } catch (final QueryEvaluationException e) {
            //False returned because for whatever reason, the ValueExpr could not be evaluated.
            //In the event that the ValueExpr is a FunctionCall, this Exception will be generated if
            //the Function URI is a valid URI that was found in the FunctionRegistry, but the arguments
            //for that Function could not be parsed.
            log.error("Could not evaluate a Filter.", e);
            return false;
        }
    }
}