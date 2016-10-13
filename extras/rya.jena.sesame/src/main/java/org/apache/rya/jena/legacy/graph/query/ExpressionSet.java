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

import org.apache.jena.util.CollectionFactory;

/**
 * ExpressionSet: represent a set of (boolean) expressions ANDed together.
 */
public class ExpressionSet {
    private final Set<Expression> expressions = CollectionFactory.createHashedSet();

    /**
     * Initialize an expression set with no members.
     */
    public ExpressionSet() {
    }

    /**
     * Adds the expression by ANDing it into the {@link ExpressionSet}.
     * @param expression the {@link Expression} to AND into the set.
     * @return this {@link ExpressionSet}
     */
    public ExpressionSet add(final Expression expression) {
        expressions.add(expression);
        return this;
    }

    /**
     * @return {@code true} if this {@link ExpressionSet} is non-trivial
     * (ie non-empty). {@code false} otherwise.
     */
    public boolean isComplex() {
        return !expressions.isEmpty();
    }

    /**
     * @param vi the {@link VariableIndexes}.
     * @return a ValuatorSet which contains exactly the valuators for each
     * Expression in this ExpressionSet, prepared against the VariableIndexes vi.
     */
    public ValuatorSet prepare(final VariableIndexes vi) {
        final ValuatorSet result = new ValuatorSet();
        final Iterator<Expression> it = expressions.iterator();
        while (it.hasNext()) {
            result.add(it.next().prepare(vi));
        }
        return result;
    }

    /**
     * @return an {@link Iterator} over all the {@link Expression}s in this
     * {@link ExpressionSet}.
     */
    public Iterator<Expression> iterator() {
        return expressions.iterator();
    }

    /**
     * @return a string representing this ExpressionSet for human consumption.
     */
    @Override
    public String toString() {
        return expressions.toString();
    }
}