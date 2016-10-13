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
package org.apache.rya.jena.legacy.graph.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphEventManager;
import org.apache.jena.graph.GraphEvents;
import org.apache.jena.graph.GraphUtil;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.util.IteratorCollection;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.rya.jena.legacy.graph.BulkUpdateHandler;

/**
 * A simple-minded implementation of the bulk update interface. This only
 * operates on (subclasses of) {@code GraphBase}, since it needs access to the
 * performAdd/performDelete operations.
 * <p>
 * It handles update events, with a special eye to not copying iterators unless
 * there is at least one listener registered with the graph's event manager.
 */
public class SimpleBulkUpdateHandler implements BulkUpdateHandler {
    protected GraphWithPerform graph;
    protected GraphEventManager manager;

    /**
     * Creates a new instance of {@link SimpleBulkUpdateHandler}.
     * @param graph the {@link GraphWithPerform}.
     */
    public SimpleBulkUpdateHandler(final GraphWithPerform graph) {
        this.graph = graph;
        this.manager = graph.getEventManager();
    }

    @Override
    @Deprecated
    public void add(final Triple[] triples) {
        for (final Triple triple : triples) {
            graph.performAdd(triple);
        }
        manager.notifyAddArray(graph, triples);
    }

    @Override
    @Deprecated
    public void add(final List<Triple> triples) {
        add(triples, true);
    }

    protected void add(final List<Triple> triples, final boolean notify) {
        for (final Triple triple : triples) {
            graph.performAdd(triple);
        }
        if (notify) {
            manager.notifyAddList(graph, triples);
        }
    }

    @Override
    @Deprecated
    public void add(final Iterator<Triple> it) {
        addIterator(it, true);
    }

    /**
     * Adds the triple iterator.
     * @param it the {@link Triple} {@link Iterator}.
     * @param notify {@code true} to notify the manager of the iterator that was
     * added. {@code false} otherwise.
     */
    public void addIterator(final Iterator<Triple> it, final boolean notify) {
        final List<Triple> s = IteratorCollection.iteratorToList(it);
        add(s, false);
        if (notify) {
            manager.notifyAddIterator(graph, s);
        }
    }

    @Override
    @Deprecated
    public void add(final Graph g) {
        addIterator(GraphUtil.findAll(g), false);
        manager.notifyAddGraph(graph, g);
    }


    @Override
    @Deprecated
    public void add(final Graph g, final boolean withReifications) {
        // Now Standard reification is the only mode, just add into the graph.
        add(g);
    }


    @Override
    @Deprecated
    public void delete(final Triple[] triples) {
        for (final Triple triple : triples) {
            graph.performDelete(triple);
        }
        manager.notifyDeleteArray(graph, triples);
    }

    @Override
    @Deprecated
    public void delete(final List<Triple> triples) {
        delete(triples, true);
    }

    protected void delete(final List<Triple> triples, final boolean notify) {
        for (final Triple triple : triples) {
            graph.performDelete(triple);
        }
        if (notify) {
            manager.notifyDeleteList(graph, triples);
        }
    }

    @Override
    @Deprecated
    public void delete(final Iterator<Triple> it) {
        deleteIterator(it, true);
    }

    /**
     * Deletes the triple iterator.
     * @param it the {@link Triple} {@link Iterator}.
     * @param notify {@code true} to notify the manager of the iterator that was
     * deleted. {@code false} otherwise.
     */
    public void deleteIterator(final Iterator<Triple> it, final boolean notify) {
        final List<Triple> triples = IteratorCollection.iteratorToList(it);
        delete(triples, false);
        if (notify) {
            manager.notifyDeleteIterator(graph, triples);
        }
    }

    private static List<Triple> triplesOf(final Graph g) {
        final ArrayList<Triple> triples = new ArrayList<>();
        final Iterator<Triple> it = g.find(Triple.ANY);
        while (it.hasNext()) {
            triples.add(it.next());
        }
        return triples;
    }

    @Override
    @Deprecated
    public void delete(final Graph g) {
        delete(g, false);
    }

    @Override
    @Deprecated
    public void delete(final Graph g, final boolean withReifications) {
        if (g.dependsOn(graph)) {
            delete(triplesOf(g));
        } else {
            deleteIterator(GraphUtil.findAll(g), false);
        }
        manager.notifyDeleteGraph(graph, g);
    }

    @Override
    @Deprecated
    public void removeAll() {
        removeAll(graph);
        notifyRemoveAll();
    }

    protected void notifyRemoveAll() {
        manager.notifyEvent(graph, GraphEvents.removeAll);
    }

    @Override
    @Deprecated
    public void remove(final Node s, final Node p, final Node o) {
        removeAll(graph, s, p, o);
        manager.notifyEvent(graph, GraphEvents.remove(s, p, o));
    }

    /**
     * Removes all the triples from the graph that match the supplied triple.
     * @param g the {@link Graph}.
     * @param s the subject {@link Node}.
     * @param p the predicate {@link Node}.
     * @param o the object {@link Node}.
     */
    public static void removeAll(final Graph g, final Node s, final Node p, final Node o) {
        final ExtendedIterator<Triple> it = g.find(s, p, o);
        try {
            while (it.hasNext()) {
                it.next();
                it.remove();
            }
        } finally {
            it.close();
        }
    }

    /**
     * Removes all triples from the graph.
     * @param g the {@link Graph}.
     */
    public static void removeAll(final Graph g) {
        final ExtendedIterator<Triple> it = GraphUtil.findAll(g);
        try {
            while (it.hasNext()) {
                it.next();
                it.remove();
            }
        } finally {
            it.close();
        }
    }
}