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

import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.shared.BrokenException;

/**
 * A QueryNode is a wrapped node that has been processed against
 * some variable-binding map to discover (a) what kind of node it is
 * and (b) what index in the binding map it has.
 * <p>
 * There are five sub-classes of QueryNode
 *
 * <ul>
 *   <li>Fixed - a concrete node, with pseudo-index NO_INDEX
 *   <li>Bind - a variable node's first, binding, appearance
 *   <li>JustBound - a variable node's bound appearance in the same
 *        [triple] context as its binding appearance
 *   <li>Bound - a variable node's bound appearance
 *   <li>Any - Node.ANY (or, in principle, a free variable)
 * </ul>
 */
public abstract class QueryNode {
    /**
     * Internal exception to throw if, against all chance, something that
     * shouldn't be involved in a match <i>is</i>.
     */
    public class MustNotMatchException extends BrokenException {
        private static final long serialVersionUID = 1L;

        /**
         * Creates a new instance of {@link MustNotMatchException}.
         * @param message the exception message.
         */
        public MustNotMatchException(final String message) {
            super(message);
        }
    }

    /**
     * Fake index value to use when no index makes sense; we choose a value
     * that will fail any array-bound check if it happens to be used anyway.
     */
    public static final int NO_INDEX = -1;

    /**
     * The Node value on which this QueryNode is based.
     */
    public final Node node;

    /**
     * The index value allocated to this query node; NO_INDEX except for a
     * variable node, in which case it is the allocated index in the domain
     * object.
     */
    public final int index;

    /**
     * Creates a new instance of {@link QueryNode}.
     * @param node the {@link Node}.
     */
    protected QueryNode(final Node node) {
        this(node, NO_INDEX);
    }

    /**
     * Creates a new instance of {@link QueryNode}.
     * @param node the {@link Node}.
     * @param index the index
     */
    protected QueryNode(final Node node, final int index) {
        this.node = node;
        this.index = index;
    }

    /**
     * @return a handy string representation for debugging purposes. Not for
     * machine consumption.
     */
    @Override
    public String toString() {
        return node.toString() + "[" + index + "]";
    }

    /**
     * @return {@code true} if this node is "frozen", ie its value is fixed, when it
     * is encountered; that is, it is not a Bind or JustBound node.
     */
    public boolean isFrozen() {
        return true;
    }

    /**
     * @param d the {@link Domain}.
     * @return a Node value to use when this QueryValue is used to select
     * objects in a Graph::find() operation; for concrete nodes, that very
     * node, for variables their current value (ANY if not bound).
     */
    public Node finder(final Domain d) {
        return Node.ANY;
    }

    /**
     * @return {@code true} if this QueryNode must be used in a triple-match of its
     * owning QueryTriple.
     */
    public boolean mustMatch() {
        return false;
    }

    /**
     * @param d the {@link Domain}.
     * @param node the {@link Node}.
     * @return {@code true} if this QueryNode matches, in the context of the binding
     * Domain {@code d}, the node {@code node}.
     */
    public boolean match(final Domain d, final Node node) {
        throw new MustNotMatchException("QueryNode " + this + " cannot match");
    }

    /**
     * Optimization: the action to be performed when matching a just-bound
     * variable or binding a newly-bound variable, or nothing for any other
     * kind of QueryNode.
     * @param d the {@link Domain}.
     * @param node the {@link Node}.
     * @return {@code true} to match or bind. {@code false} to do nothing.
     */
    public abstract boolean matchOrBind(Domain d, Node node);

    /**
     * @param f the {@link QueryNodeFactory}.
     * @param map the {@link Mapping}.
     * @param recent the recent {@link Set} of {@link Node}s.
     * @param n the {@link Node}.
     * @return a QueryNode that classifies the argument node {@code n}.
     * The factory {@code f} is used to create the different QueryNodes,
     * allowing different classifiers to use their own subclasses of QueryNode
     * if they wish. {@code map} is the variable-to-index map, and
     * {@code recent} is the set of those variables "just" bound, ie,
     * earlier in the same triple.
     */
    public static QueryNode classify(final QueryNodeFactory f, final Mapping map, final Set<Node> recent, final Node n) {
        if (n.equals(Node.ANY)) {
            return f.createAny();
        }
        if (n.isVariable()) {
            if (map.hasBound(n)) {
                if (recent.contains(n)) {
                    return f.createJustBound(n, map.indexOf(n));
                } else {
                    return f.createBound(n, map.indexOf(n));
                }
            } else {
                recent.add(n);
                return f.createBind(n, map.newIndex(n));
            }
        }
        return new Fixed(n);
    }

    public static final QueryNodeFactory FACTORY = new QueryNodeFactoryBase();

    public static class Fixed extends QueryNode {
        /**
         * Creates a new instance of {@link Fixed}.
         * @param n the {@link Node}.
         */
        public Fixed(final Node n) {
            super(n);
        }

        @Override
        public Node finder(final Domain d) {
            return node;
        }

        @Override
        public boolean matchOrBind(final Domain d, final Node node) {
            return node.matches(node);
        }

        @Override
        public String toString() {
            return node.toString() + "[fixed]";
        }
    }

    public static class Bind extends QueryNode {
        /**
         * Creates a new instance of {@link Bind}.
         * @param n the {@link Node}.
         * @param index the index.
         */
        public Bind(final Node n, final int index) {
            super(n, index);
        }

        @Override
        public boolean mustMatch() {
            return true;
        }

        @Override
        public boolean isFrozen() {
            return false;
        }

        @Override
        public boolean match(final Domain d, final Node value) {
            d.setElement(index, value);
            return true;
        }

        @Override
        public boolean matchOrBind(final Domain d, final Node value) {
            d.setElement(index, value);
            return true;
        }

        @Override
        public String toString() {
            return node.toString() + "[bind " + index + "]";
        }
    }

    public static class JustBound extends QueryNode {
        /**
         * Creates a new instance of {@link JustBound}.
         * @param n the {@link Node}.
         * @param index the index.
         */
        public JustBound(final Node n, final int index) {
            super(n, index);
        }

        @Override
        public boolean mustMatch() {
            return true;
        }

        @Override
        public boolean isFrozen() {
            return false;
        }

        @Override
        public boolean match(final Domain d, final Node node) {
            return node.matches(d.getElement(index));
        }

        @Override
        public boolean matchOrBind(final Domain d, final Node node) {
            return node.matches(d.getElement(index));
        }

        @Override
        public String toString() {
            return node.toString() + "[just " + index + "]";
        }
    }

    public static class Bound extends QueryNode {
        /**
         * Creates a new instance of {@link Bound}.
         * @param n the {@link Node}.
         * @param index the index.
         */
        public Bound(final Node n, final int index) {
            super(n, index);
        }

        @Override
        public Node finder(final Domain d) {
            return d.getElement(index);
        }

        @Override
        public boolean matchOrBind(final Domain d, final Node node) {
            return d.getElement(index).matches(node);
        }

        @Override
        public String toString() {
            return node.toString() + "[bound " + index + "]";
        }
    }

    public static class Any extends QueryNode {
        /**
         * Creates a new instance of {@link Any}.
         */
        public Any() {
            super(Node.ANY);
        }

        @Override
        public boolean matchOrBind(final Domain d, final Node node) {
            return true;
        }

        @Override
        public String toString() {
            return "ANY";
        }
    }
}