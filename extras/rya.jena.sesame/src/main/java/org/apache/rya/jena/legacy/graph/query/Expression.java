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

import java.util.HashSet;
import java.util.Set;

/**
 * Expression - the interface for expressions that is expected by Query for
 * constraints. An Expression can be evaluated (given a name->value mapping);
 * it can be prepared into a Valuator (given a name->index mapping); and it can
 * be analyzed into its components.
 * <p>
 * An Expression can be a variable, an application, or a literal value. If an
 * access method (eg getName) is applied to an Expression for which it is not
 * appropriate (eg an application), <em>the result is unspecified</em>; an
 * implementation is free to throw an exception, deliver a null result, deliver
 * a misleading value, whatever is convenient.
 * <p>
 * The nested class {link Util} provides some expression utility
 * methods, including a generic version of {@link prepare(VariableIndexes)}. The
 * nested abstract class {@link Base} and its sub-classes {@link Literal},
 * {@link Variable}, and {@link Application} provide a framework
 * for developers to implement Expressions over.
 */
public interface Expression {
    /**
     * @param vi the {@link VariableIndexes}.
     * @return a {@link Valuator} which, when run with a set of index-to-value
     * bindings, evaluates this expression in the light of the given
     * variable-to-index bindings [ie as though the variables were bound to the
     * corresponding values]
     */
    public Valuator prepare(VariableIndexes vi);

    /**
     * @return {@code true} if this {@link Expression} represents a variable.
     */
    public boolean isVariable();

    /**
     * If this Expression is a variable, answer a [non-null] String which is its
     * name. Otherwise the behavior is unspecified.
     * @return the name.
     */
    public String getName();

    /**
     * @return {@code true} if this Expression represents a literal
     * [Java object] value.
     */
    public boolean isConstant();

    /**
     * If this Expression is a literal, answer the value of that literal.
     * Otherwise the behavior is unspecified.
     * @return the value.
     */
    public Object getValue();

    /**
     * @return {@code true} if this Expression represents the application of
     * some function [or operator] to some arguments [or operands].
     */
    public boolean isApply();

    /**
     * If this Expression is an application, return the string identifying the
     * function, which should be a URI. Otherwise the behavior is unspecified.
     * @return the function string.
     */
    public String getFun();

    /**
     * If this Expression is an application, answer the number of arguments that
     * it has. Otherwise the behavior is unspecified.
     * @return the number of arguments.
     */
    public int argCount();

    /**
     * If this Expression is an application, and 0 &lt;= i &lt; argCount(),
     * answer the {@code i} the argument. Otherwise the behavior is unspecified.
     * @param i the argument count.
     * @return the {@link Expression}.
     */
    public Expression getArg(int i);

    /**
     * An Expression which always evaluates to {@code true}.
     */
    public static Expression TRUE = new BoolConstant(true);

    /**
     * An Expression which always evaluates to {@code false}.
     */
    public static Expression FALSE = new BoolConstant(false);

    /**
     * An abstract base class for Expressions; over-ride as appropriate. The
     * sub-classes may be more useful.
     */
    public static abstract class Base implements Expression {
        @Override
        public boolean isVariable() {
            return false;
        }

        @Override
        public boolean isApply() {
            return false;
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public int argCount() {
            return 0;
        }

        @Override
        public String getFun() {
            return null;
        }

        @Override
        public Expression getArg(final int i) {
            return null;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Expression && Expression.Util.equals(this, (Expression) other);
        }
    }

    /**
     * An abstract base class for literal nodes; subclasses implement getValue().
     */
    public static abstract class Constant extends Base {
        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public abstract Object getValue();
    }

    /**
     * A concrete class for representing fixed constants; each instance
     * can hold a separate value and its valuator returns that value.
     */
    public static class Fixed extends Constant {
        protected Object value;

        /**
         * Creates a new instance of {@link Fixed}.
         * @param value the value.
         */
        public Fixed(final Object value) {
            this.value = value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public Valuator prepare(final VariableIndexes vi) {
            return new FixedValuator(value);
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }

    /**
     * An abstract base class for variable nodes; subclasses implement
     * {@link #getName()}.
     */
    public static abstract class Variable extends Base {
        @Override
        public boolean isVariable() {
            return true;
        }

        @Override
        public abstract String getName();
    }

    /**
     * An abstract base class for apply nodes; subclasses implement getFun(),
     * argCount(), and getArg().
     */
    public static abstract class Application extends Base {
        @Override
        public boolean isApply() {
            return true;
        }

        @Override
        public abstract int argCount();

        @Override
        public abstract String getFun();

        @Override
        public abstract Expression getArg(int i);
    }

    /**
     * Utility methods for Expressions, captured in a class because they can't
     * be written directly in the interface.
     */
    public static class Util {
        /**
         * @return a set containing exactly the names of variables within
         * {@code e}.
         */
        public static Set<String> variablesOf(final Expression e) {
            return addVariablesOf(new HashSet<String>(), e);
        }

        /**
         * Add all the variables of {@code expression} to {@code s}, and return
         * {@code s}.
         * @param s the {@link Set} of strings.
         * @param expression the variable {@link Expression}.
         * @return the {@link Set} of strings.
         */
        public static Set<String> addVariablesOf(final Set<String> s, final Expression expression) {
            if (expression.isVariable()) {
                s.add(expression.getName());
            } else if (expression.isApply()) {
                for (int i = 0; i < expression.argCount(); i += 1) {
                    addVariablesOf(s, expression.getArg(i));
                }
            }
            return s;
        }

        public static boolean containsAllVariablesOf(final Set<String> variables, final Expression e) {
            if (e.isConstant()) {
                return true;
            }
            if (e.isVariable()) {
                return variables.contains(e.getName());
            }
            if (e.isApply()) {
                for (int i = 0; i < e.argCount(); i += 1) {
                    if (containsAllVariablesOf(variables, e.getArg(i)) == false) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        /**
         * @param l the left-hand {@link Expression}.
         * @param r the right-hand {@link Expression}.
         * @return {@code true} if the 2 {@link Expression}s are equal.
         */
        public static boolean equals(final Expression l, final Expression r) {
            return l.isConstant() ? r.isConstant() && l.getValue().equals(r.getValue()) : l.isVariable() ? r.isVariable() && r.getName().equals(r.getName()) : l.isApply() ? r.isApply()
                    && sameApply(l, r) : false;
        }

        /**
         * @param l the left-hand {@link Expression}.
         * @param r the right-hand {@link Expression}.
         * @return {@code true} if the 2 {@link Expression}s have the same
         * function and arguments. {@code false} otherwise.
         */
        public static boolean sameApply(final Expression l, final Expression r) {
            return l.argCount() == r.argCount() && l.getFun().equals(r.getFun()) && sameArgs(l, r);
        }

        /**
         * @param l the left-hand {@link Expression}.
         * @param r the right-hand {@link Expression}.
         * @return {@code true} if the 2 {@link Expression}s have the same arguments.
         * {@code false} otherwise.
         */
        public static boolean sameArgs(final Expression l, final Expression r) {
            for (int i = 0; i < l.argCount(); i += 1) {
                if (!equals(l.getArg(i), r.getArg(i))) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Valof provides an implementation of VariableValues which composes the
     * "compile-time" VariableIndexes map with the "run-time" IndexValues map
     * to produce a VariableValues map. A Valof has mutable state; the setDomain
     * operation changes the IndexValues mapping of the Valof.
     */
    static class Valof implements VariableValues {
        private final VariableIndexes map;
        private IndexValues dom;

        /**
         * Creates a new instance of {@link Valof}.
         * @param map the {@link VariableIndexes} map.
         */
        public Valof(final VariableIndexes map) {
            this.map = map;
        }

        @Override
        public final Object get(final String name) {
            return dom.get(map.indexOf(name));
        }

        public final Valof setDomain(final IndexValues d) {
            dom = d;
            return this;
        }
    }

    /**
     * Base class used to implement {@code TRUE} and {@code FALSE}.
     */
    public static class BoolConstant extends Base implements Expression, Valuator {
        private final boolean value;

        /**
         * Creates a new instance of {@link BoolConstant}.
         * @param value the boolean value.
         */
        public BoolConstant(final boolean value) {
            this.value = value;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public Object getValue() {
            return Boolean.valueOf(value);
        }

        @Override
        public Valuator prepare(final VariableIndexes vi) {
            return this;
        }

        /**
         * @param vv the {@link VariableValues}.
         * @return the evaluated value as a primitive boolean.
         */
        public boolean evalBool(final VariableValues vv) {
            return value;
        }

        @Override
        public boolean evalBool(final IndexValues iv) {
            return value;
        }

        @Override
        public Object evalObject(final IndexValues iv) {
            return getValue();
        }
    }
}