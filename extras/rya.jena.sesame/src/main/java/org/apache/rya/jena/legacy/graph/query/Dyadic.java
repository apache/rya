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

import org.apache.jena.shared.JenaException;
import org.apache.rya.jena.legacy.graph.query.Expression.Application;

/**
 * A base class for dyadic expressions with a built-in Valuator; subclasses must
 * define an {@link #evalObject} or {@link #evalBool} method which will be
 * supplied with the evaluated operands.
 */
public abstract class Dyadic extends Application {
    private final Expression l;
    private final Expression r;
    private final String f;

    /**
     * Creates a new instance of {@link Dyadic}.
     * @param l the left-hand {@link Expression}.
     * @param f If this expression is an application, return the string
     * identifying the function, which should be a URI. Otherwise the behavior
     * is unspecified.
     * @param r the right-hand {@link Expression}.
     */
    public Dyadic(final Expression l, final String f, final Expression r) {
        this.l = l;
        this.f = f;
        this.r = r;
    }

    @Override
    public int argCount() {
        return 2;
    }

    @Override
    public Expression getArg(final int i) {
        return i == 0 ? l : r;
    }

    @Override
    public String getFun() {
        return f;
    }

    /**
     * @param l the left-hand argument {link Object}.
     * @param r the right-hand argument {link Object}.
     * @returns the Object result of evaluating this dyadic expression with
     * the given arguments {@code l} and {@code r}.
     * Either this method or {@link #evalBool} <i>must</i> be
     * over-ridden in concrete sub-classes.
     */
    public Object evalObject(final Object l, final Object r) {
        return evalBool(l, r) ? Boolean.TRUE : Boolean.FALSE;
    }

    /**
     * @param l the left-hand argument {link Object}.
     * @param r the right-hand argument {link Object}.
     * @return the boolean result of evaluating this dyadic expression with
     * the given arguments {@code l} and {@code r}.
     * Either this method or {@link #evalObject} <i>must</i> be
     * over-ridden in concrete sub-classes.
     */
    public boolean evalBool(final Object l, final Object r) {
        final Object object = evalObject(l, r);
        if (object instanceof Boolean) {
            return ((Boolean) object).booleanValue();
        }
        throw new JenaException("not Boolean: " + object);
    }

    @Override
    public Valuator prepare(final VariableIndexes vi) {
        final Valuator lValuator = l.prepare(vi);
        final Valuator rValuator = r.prepare(vi);
        return new Valuator() {
            @Override
            public boolean evalBool(final IndexValues iv) {
                return ((Boolean) evalObject(iv)).booleanValue();
            }

            @Override
            public Object evalObject(final IndexValues iv) {
                return Dyadic.this.evalObject(lValuator.evalObject(iv), rValuator.evalObject(iv));
            }
        };
    }

    @Override
    public String toString() {
        return l.toString() + " " + f + " " + r.toString();
    }

    /**
     * ANDs the 2 expressions together.
     * @param l the left-hand {@link Expression}.
     * @param r the right-hand {@link Expression}.
     * @return the ANDed {@link Expression}.
     */
    public static Expression and(final Expression l, final Expression r) {
        return new Dyadic(l, ExpressionFunctionURIs.AND, r) {
            @Override
            public boolean evalBool(final Object x, final Object y) {
                return ((Boolean) x).booleanValue() && ((Boolean) y).booleanValue();
            }
        };
    }
}