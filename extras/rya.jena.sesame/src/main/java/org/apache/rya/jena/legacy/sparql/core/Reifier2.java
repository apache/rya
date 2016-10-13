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
package org.apache.rya.jena.legacy.sparql.core;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jena.atlas.iterator.Filter;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.shared.AlreadyReifiedException;
import org.apache.jena.shared.CannotReifyException;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.rya.jena.legacy.graph.Reifier;
import org.apache.rya.jena.legacy.shared.ReificationStyle;

/**
 * A Reifier that only supports one style Standard (intercept, no conceal
 * -- and intercept is a no-op anyway because all triples
 * appear in the underlying graph for storing all triples).
 */
public class Reifier2 implements Reifier {
    private static final String QS = "PREFIX rdf: <" + RDF.getURI() + ">\n" +
            "SELECT * \n" +
            "{ ?x rdf:type rdf:Statement; rdf:subject ?S; rdf:predicate ?P; rdf:object ?O }";
    private static final Query QUERY = QueryFactory.create(QS);
    private static final Op OP = Algebra.compile(QUERY);
    private static final Var REIF_NODE_VAR = Var.alloc("x");
    private static final Var VAR_S = Var.alloc("S");
    private static final Var VAR_P = Var.alloc("P");
    private static final Var VAR_O = Var.alloc("O");

    private static final Node RDF_TYPE     = RDF.Nodes.type;
    private static final Node STATEMENT    = RDF.Nodes.Statement;
    private static final Node SUBJECT      = RDF.Nodes.subject;
    private static final Node PREDICATE    = RDF.Nodes.predicate;
    private static final Node OBJECT       = RDF.Nodes.object;

    private final Graph graph;
    private final DatasetGraph ds;
    private final QueryEngineFactory factory;

    /**
     * Creates a new instance of {@link Reifier2}.
     * @param graph the {@link Graph}.
     */
    public Reifier2(final Graph graph) {
        this.graph = graph;
        this.ds = DatasetGraphFactory.createOneGraph(graph);
        this.factory = QueryEngineRegistry.findFactory(OP, ds, null);
    }

    @Override
    public ExtendedIterator<Node> allNodes() {
        return allNodes(null);
    }

    private static class MapperToNode extends NiceIterator<Node> {
        private final QueryIterator iter;
        private final Var var;

        /**
         * Creates a new instance of {@link MapperToNode}.
         * @param iter the {@link QueryIterator}.
         * @param var the {@link Var}.
         */
        MapperToNode(final QueryIterator iter, final Var var) {
            this.iter = iter;
            this.var = var;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Node next() {
            final Binding b = iter.nextBinding();
            final Node n = b.get(var);
            return n;
        }

        @Override
        public void close() {
            iter.close();
        }
    }

    @Override
    public ExtendedIterator<Node> allNodes(final Triple triple) {
        final QueryIterator qIter = nodesReifTriple((Node)null, triple);
        return new MapperToNode(qIter, REIF_NODE_VAR);
    }

    private QueryIterator nodesReifTriple(Node node, final Triple triple) {
        final Binding b = BindingRoot.create();

        if (node == Node.ANY) {
            node = null;
        }

        if (node != null || triple != null) {
            final BindingHashMap bhm = new BindingHashMap(b);
            if (node != null) {
                bind(bhm, REIF_NODE_VAR, node);
            }
            if (triple != null) {
                bind(bhm, VAR_S, triple.getMatchSubject());
                bind(bhm, VAR_P, triple.getMatchPredicate());
                bind(bhm, VAR_O, triple.getMatchObject());
            }
        }

        final Plan plan = factory.create(OP, ds, b, null);
        final QueryIterator qIter = plan.iterator();
        return qIter;
    }

    private static void bind(final BindingHashMap b, final Var var, final Node node) {
        if (node == null || node == Node.ANY) {
            return;
        }
        b.add(var, node);
    }

    @Override
    public void close() {
    }

    private static class MapperToTriple extends NiceIterator<Triple> {
        private final QueryIterator iter;

        /**
         * Creates a new instance of {@link MapperToTriple}.
         * @param iter the {@link QueryIterator}.
         */
        MapperToTriple(final QueryIterator iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Triple next() {
            final Binding b = iter.nextBinding();
            final Node s = b.get(VAR_S);
            final Node p = b.get(VAR_P);
            final Node o = b.get(VAR_O);
            return new Triple(s, p, o);
        }

        @Override
        public void close() {
            iter.close();
        }
    }

    @Override
    public ExtendedIterator<Triple> find(final Triple match) {
        return graph.find(match);
//        QueryIterator qIter = nodesReifTriple(null, match);
//        // To ExtendedIterator.
//        return new MapperToTriple(qIter);
    }

    @Override
    public ExtendedIterator<Triple> findEither(final Triple match, final boolean showHidden) {
        if (showHidden) {
            return NullIterator.instance();
        } else {
            return graph.find(match);
        }
    }


    static Filter<Triple> filterReif = new Filter<Triple>() {
        @Override
        public boolean test(final Triple triple) {
            return triple.getPredicate().equals(SUBJECT) ||
                   triple.getPredicate().equals(PREDICATE) ||
                   triple.getPredicate().equals(OBJECT) ||
                   (triple.getPredicate().equals(RDF_TYPE) && triple.getObject().equals(STATEMENT));
        }};

    @Override
    public ExtendedIterator<Triple> findExposed(final Triple match) {
        Iterator<Triple> it = graph.find(match);
        it = Iter.filter(it, filterReif);
        return WrappedIterator.create(it);
    }

    @Override
    public Graph getParentGraph() {
        return graph;
    }

    @Override
    public ReificationStyle getStyle() {
        return ReificationStyle.Standard;
    }

    @Override
    public boolean handledAdd(final Triple triple) {
        graph.add(triple);
        return true;
    }

    @Override
    public boolean handledRemove(final Triple triple) {
        graph.delete(triple);
        return true;
    }

    @Override
    public boolean hasTriple(final Node node) {
        return getTriple(node) != null;
    }

    @Override
    public boolean hasTriple(final Triple triple) {
        final QueryIterator qIter = nodesReifTriple(null, triple);
        try {
            if (! qIter.hasNext()) {
                return false;
            }
            final Binding b = qIter.nextBinding();
            final Node node = b.get(REIF_NODE_VAR);
            if (qIter.hasNext()) {
                // Over specified
                return false;
            }
            // This checks there are no fragments
            return getTriple(node) != null;
        } finally {
            qIter.close();
        }
    }

    @Override
    public Node reifyAs(Node node, final Triple triple) {
        if (node == null) {
            node = NodeFactory.createBlankNode();
        } else {
            final Triple t = getTriple(node);

            if (t != null && ! t.equals(triple)) {
                throw new AlreadyReifiedException(node);
            }
            if (t != null) {
                // Already there
                return node;
            }
        }

        graph.add(new Triple(node, RDF_TYPE, STATEMENT));
        graph.add(new Triple(node, SUBJECT, triple.getSubject()));
        graph.add(new Triple(node, PREDICATE, triple.getPredicate()));
        graph.add(new Triple(node, OBJECT, triple.getObject()));

        // Check it's a well-formed reification by Jena's uniqueness rules
        final Triple t = getTriple(node);
        if (t == null) {
            throw new CannotReifyException(node);
        }
        return node;
    }

    @Override
    public void remove(final Triple triple) {
        remove(null, triple);
    }

    @Override
    public void remove(Node node, final Triple triple) {
        if (node == null) {
            node = Node.ANY;
        }

        //QueryIterator qIter = nodesReifTriple(node, triple);
        final Set<Triple> triples = new HashSet<Triple>();
        triplesToZap(triples, node, RDF_TYPE, STATEMENT);
        triplesToZap(triples, node, SUBJECT, triple.getSubject());
        triplesToZap(triples, node, PREDICATE, triple.getPredicate());
        triplesToZap(triples, node, OBJECT, triple.getObject());
        for (final Triple t : triples) {
            graph.delete(t);
        }
    }

    private void triplesToZap(final Collection<Triple> acc, final Node s, final Node p , final Node o) {
        final ExtendedIterator<Triple> iter = graph.find(s, p, o);
        while(iter.hasNext()) {
            acc.add(iter.next());
        }
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Triple getTriple(final Node node) {
        final QueryIterator qIter = nodesReifTriple(node, null);
        try {
            if (!qIter.hasNext()) {
                return null;
            }
            final Binding b = qIter.nextBinding();
            if (qIter.hasNext()) {
                // Over specified
                return null;
            }
            // Just right
            final Node s = b.get(VAR_S);
            final Node p = b.get(VAR_P);
            final Node o = b.get(VAR_O);
            return new Triple(s, p, o);
        } finally {
            qIter.close();
        }
    }

    private Node getNode(final Node s, final Node p) {
        final ExtendedIterator<Triple> it = graph.find(s, p, Node.ANY);
        if (!it.hasNext()) {
            return null;
        }
        final Triple t = it.next();
        it.close();
        return t.getObject();
    }
}