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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.CollectionFactory;
import org.apache.jena.util.iterator.ClosableIterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

/**
 * A SimpleQueryHandler is a more-or-less straightforward implementation of QueryHandler
 * suitable for use on graphs with no special query engines.
 */
public class SimpleQueryHandler implements QueryHandler {
    /**
     * the Graph this handler is working for
     */
    protected Graph graph;

    /**
     * make an instance, remember the graph
     */
    public SimpleQueryHandler(final Graph graph) {
        this.graph = graph;
    }

    @Override
    public Stage patternStage(final Mapping map, final ExpressionSet constraints, final Triple[] t) {
        return new PatternStage(graph, map, constraints, t);
    }

    @Override
    public BindingQueryPlan prepareBindings(final GraphQuery q, final Node[] variables) {
        return new SimpleQueryPlan(graph, q, variables);
    }

    @Override
    public TreeQueryPlan prepareTree(final Graph pattern) {
        return new SimpleTreeQueryPlan(graph, pattern);
    }

    @Override
    public ExtendedIterator<Node> objectsFor(final Node s, final Node p) {
        return objectsFor(graph, s, p);
    }

    @Override
    public ExtendedIterator<Node> subjectsFor(final Node p, final Node o) {
        return subjectsFor(graph, p, o);
    }

    @Override
    public ExtendedIterator<Node> predicatesFor(final Node s, final Node o) {
        return predicatesFor(graph, s, o);
    }

    public static ExtendedIterator<Node> objectsFor(final Graph g, final Node s, final Node p) {
        final Set<Node> objects = CollectionFactory.createHashedSet();
        final ClosableIterator<Triple> it = g.find(s, p, Node.ANY);
        while (it.hasNext()) {
            objects.add(it.next().getObject());
        }
        return WrappedIterator.createNoRemove(objects.iterator());
    }

    public static ExtendedIterator<Node> subjectsFor(final Graph g, final Node p, final Node o) {
        final Set<Node> objects = CollectionFactory.createHashedSet();
        final ClosableIterator<Triple> it = g.find(Node.ANY, p, o);
        while (it.hasNext()) {
            objects.add(it.next().getSubject());
        }
        return WrappedIterator.createNoRemove(objects.iterator());
    }

    public static ExtendedIterator<Node> predicatesFor(final Graph g, final Node s, final Node o) {
        final Set<Node> predicates = CollectionFactory.createHashedSet();
        final ClosableIterator<Triple> it = g.find(s, Node.ANY, o);
        while (it.hasNext()) {
            predicates.add(it.next().getPredicate());
        }
        return WrappedIterator.createNoRemove(predicates.iterator());
    }

    /**
     * this is a simple-minded implementation of containsNode that uses find
     * up to three times to locate the node. Almost certainly particular graphs
     * will be able to offer better query-handlers ...
     */
    @Override
    public boolean containsNode(final Node n) {
        return graph.contains(n, Node.ANY, Node.ANY) || graph.contains(Node.ANY, n, Node.ANY) || graph.contains(Node.ANY, Node.ANY, n);
    }

    final public static Map<Graph, SimpleQueryHandler> grMap = new HashMap<Graph, SimpleQueryHandler>();

    public static SimpleQueryHandler findOrCreate(final Graph g) {
        synchronized (grMap) {
            SimpleQueryHandler queryHandler = grMap.get(g);
            if (queryHandler == null) {
                queryHandler = new SimpleQueryHandler(g);
                grMap.put(g, queryHandler);
            }
            return queryHandler;
        }
    }
}
