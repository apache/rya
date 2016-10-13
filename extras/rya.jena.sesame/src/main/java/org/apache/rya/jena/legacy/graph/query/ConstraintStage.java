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

import org.apache.log4j.Logger;

/**
 * A ConstraintStage implements the constraint evaluation part of a
 * query. Any constraints not handled by previous PatternStages are prepared
 * against the mapping and valuated against each binding that comes down
 * the pipe; only bindings that evaluate to {@code true} are passed onward.
 */
public class ConstraintStage extends Stage {
    private static final Logger log = Logger.getLogger(ConstraintStage.class);

    /**
     * The set of prepared Valuators representing the constraint.
     */
    protected ValuatorSet prepared;

    /**
     * Initialize this {@link ConstraintStage} with the mapping [from names to
     * indexes] and {@link ExpressionSet} [the constraint expressions] that will
     * be evaluated when the constraint stage runs.
     * @param map {@link Mapping} from names to indexes.
     * @param constraint the {@link ExpressionSet} of constraints.
     */
    public ConstraintStage(final Mapping map, final ExpressionSet constraint) {
        this.prepared = constraint.prepare(map);
    }

    /**
     * Evaluate the prepared constraints with the values given by the domain.
     * @param domain the {@link Domain}.
     * @param valuatorSet the {@link ValuatorSet}.
     * @return {@code true} if the constraint evaluates to {@code true}, and
     * {@code false} if it evaluates to {@code false} or throws an exception.
     */
    private static boolean evalConstraint(final Domain domain, final ValuatorSet valuatorSet) {
        try {
            return valuatorSet.evalBool(domain);
        } catch (final Exception e) {
            log.error("Encountered an exception while evaluating the constaint.", e);
            return false;
        }
    }

    /**
     * The delivery component: read the domain elements out of the
     * input pipe, and only pass on those that satisfy the predicate.
     */
    @Override
    public Pipe deliver(final Pipe pipe) {
        final Pipe mine = previous.deliver(new BufferPipe());
        final Thread thread = new Thread("a ConstraintStage") {
            @Override
            public void run() {
                while (mine.hasNext()) {
                    final Domain domain = mine.get();
                    if (evalConstraint(domain, prepared)) {
                        pipe.put(domain);
                    }
                }
                pipe.close();
            }
        };
        thread.start();
        return pipe;
    }
}