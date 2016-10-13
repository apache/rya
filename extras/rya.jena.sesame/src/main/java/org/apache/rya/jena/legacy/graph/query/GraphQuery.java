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

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphUtil;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.shared.JenaException;
import org.apache.jena.util.iterator.ClosableIterator;
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 * The class of graph queries, plus some machinery (which should move) for
 * implementing them.
 */
public class GraphQuery {
    /**
     * A convenient synonym for Node.ANY, used in a match to match anything.
     */
    public static final Node ANY = Node.ANY;

    /**
     * A query variable called "S".
     */
    public static final Node S = NodeFactory.createVariable("S");

    /**
     * A query variable called "P".
     */
    public static final Node P = NodeFactory.createVariable("P");

    /**
     * A query variable called "O".
     */
    public static final Node O = NodeFactory.createVariable("O");

    /**
     * A query variable called "X".
     */
    public static final Node X = NodeFactory.createVariable("X");

    /**
     * A query variable called "Y".
     */
    public static final Node Y = NodeFactory.createVariable("Y");

    /**
     * A query variable called "Z".
     */
    public static final Node Z = NodeFactory.createVariable("Z");

    /**
     * Initializer for Query; makes an empty Query [no matches, no constraints]
     */
    public GraphQuery() {
    }

    /**
     * Initializer for Query; makes a Query with its matches taken from
     * {@code pattern}.
     * @param pattern a Graph whose triples are used as match elements
     */
    public GraphQuery(final Graph pattern) {
        addMatches(pattern);
    }

    /**
     * Exception thrown when a query variable is discovered to be unbound.
     */
    public static class UnboundVariableException extends JenaException {
        private static final long serialVersionUID = 1L;

        /**
         * Creates a new instance of {@link UnboundVariableException}.
         * @param n the {@link Node}.
         */
        public UnboundVariableException(final Node n) {
            super(n.toString());
        }
    }

    /**
     * Add an (S, P, O) match to the query's collection of match triples. Return
     * this query for cascading.
     * @param s the node to match the subject
     * @param p the node to match the predicate
     * @param o the node to match the object
     * @return this Query, for cascading
     */
    public GraphQuery addMatch(final Node s, final Node p, final Node o) {
        return addNamedMatch(NamedTripleBunches.anon, s, p, o);
    }

    /**
     * Add a triple to the query's collection of match triples.
     * @param t an (S, P, O) triple to add to the collection of matches.
     * @return this {@link GraphQuery}, for cascading.
     */
    public GraphQuery addMatch(final Triple t) {
        triplePattern.add(t);
        triples.add(NamedTripleBunches.anon, t);
        return this;
    }

    private GraphQuery addNamedMatch(final String name, final Node s, final Node p, final Node o) {
        triplePattern.add(Triple.create(s, p, o));
        triples.add(name, Triple.create(s, p, o));
        return this;
    }

    /**
     * The named bunches of triples for graph matching
     */
    private final NamedTripleBunches triples = new NamedTripleBunches();

    private final List<Triple> triplePattern = new ArrayList<Triple>();

    /**
     * @return a list of the triples that have been added to this query.
     * (Note: ignores "named triples").
     */
    public List<Triple> getPattern() {
        return new ArrayList<Triple>(triplePattern);
    }

    private final ExpressionSet constraint = new ExpressionSet();

    /**
     * @return the {@link ExpressionSet} constraints.
     */
    public ExpressionSet getConstraints() {
        return constraint;
    }

    /**
     * Adds a an expression constraint the the graph.
     * @param e the {@link Expression} constraint
     * @return this {@link GraphQuery}.
     */
    public GraphQuery addConstraint(final Expression e) {
        if (e.isApply() && e.getFun().equals(ExpressionFunctionURIs.AND)) {
            for (int i = 0; i < e.argCount(); i += 1) {
                addConstraint(e.getArg(i));
            }
        } else if (e.isApply() && e.argCount() == 2 && e.getFun().equals(ExpressionFunctionURIs.Q_StringMatch)) {
            constraint.add(Rewrite.rewriteStringMatch(e));
        } else {
            constraint.add(e);
        }
        return this;
    }

    /**
     * Add all the (S, P, O) triples of {@code graph} to this Query as matches.
     * @param graph the graph matches to add.
     */
    private void addMatches(final Graph graph) {
        final ClosableIterator<Triple> it = GraphUtil.findAll(graph);
        while (it.hasNext()) {
            addMatch(it.next());
        }
    }

    /**
     * Executes the bindings.
     * @param g the {@link Graph}.
     * @param results the array of {@link Node} results.
     * @return the {@link ExtendedIterator} over the {@link Domain}s.
     */
    public ExtendedIterator<Domain> executeBindings(final Graph g, final Node[] results) {
        return executeBindings(args().put(NamedTripleBunches.anon, g), results);
    }

    /**
     * Executes the bindings.
     * @param g the {@link Graph}.
     * @param stages the {@link List} of {@link Stage}s.
     * @param results results the array of {@link Node} results.
     * @return the {@link ExtendedIterator} over the {@link Domain}s.
     */
    public ExtendedIterator<Domain> executeBindings(final Graph g, final List<Stage> stages, final Node[] results) {
        return executeBindings(stages, args().put(NamedTripleBunches.anon, g), results);
    }

    /**
     * Executes the bindings.
     * @param args the {@link NamedGraphMap} args.
     * @param nodes the array of {@link Node}s.
     * @return the {@link ExtendedIterator} over the {@link Domain}s.
     */
    public ExtendedIterator<Domain> executeBindings(final NamedGraphMap args, final Node[] nodes) {
        return executeBindings(new ArrayList<Stage>(), args, nodes);
    }

    /**
     * The standard "default" implementation of executeBindings.
     * @param outStages stages the {@link List} of {@link Stage}s.
     * @param args the {@link NamedGraphMap} args.
     * @param nodes the array of {@link Node}s.
     * @return the {@link ExtendedIterator} over the {@link Domain}s.
     */
    public ExtendedIterator<Domain> executeBindings(final List<Stage> outStages, final NamedGraphMap args, final Node[] nodes) {
        final SimpleQueryEngine e = new SimpleQueryEngine(triplePattern, sortMethod, constraint);
        final ExtendedIterator<Domain> result = e.executeBindings(outStages, args, nodes);
        lastQueryEngine = e;
        return result;
    }

    private SimpleQueryEngine lastQueryEngine = null;

    /**
     * mapping of graph name -> graph
     */
    private final NamedGraphMap argMap = new NamedGraphMap();

    /**
     * @return the {@link NamedGraphMap} args.
     */
    public NamedGraphMap args() {
        return argMap;
    }

    /**
     * @return the {@link TripleSorter}.
     */
    public TripleSorter getSorter() {
        return sortMethod;
    }

    /**
     * Sets the {@link TripleSorter}.
     * @param ts the {@link TripleSorter}.
     */
    public void setTripleSorter(final TripleSorter ts) {
        sortMethod = ts == null ? TripleSorter.dontSort : ts;
    }

    private TripleSorter sortMethod = TripleSorter.dontSort;

    /**
     * @return the variable count.
     */
    public int getVariableCount() {
        return lastQueryEngine.getVariableCount();
    }
}