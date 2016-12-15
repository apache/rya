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
package org.apache.rya.jena.legacy.sparql.graph;

import org.apache.jena.graph.Capabilities;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphEventManager;
import org.apache.jena.graph.GraphStatisticsHandler;
import org.apache.jena.graph.GraphUtil;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.TransactionHandler;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.AllCapabilities;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.graph.impl.GraphMatcher;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.graph.impl.SimpleEventManager;
import org.apache.jena.graph.impl.SimpleTransactionHandler;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.ClosedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.util.iterator.ClosableIterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.rya.jena.legacy.graph.BulkUpdateHandler;
import org.apache.rya.jena.legacy.graph.Reifier;
import org.apache.rya.jena.legacy.graph.impl.SimpleBulkUpdateHandler;
import org.apache.rya.jena.legacy.graph.query.QueryHandler;
import org.apache.rya.jena.legacy.sparql.core.Reifier2;

/**
 * Like GraphBase but without any reificiation handling
 */
public abstract class GraphBase2 implements GraphWithPerform {
    /**
     * Whether or not this graph has been closed - used to report ClosedExceptions
     * when an operation is attempted on a closed graph.
     */
    protected boolean closed = false;

    /**
     * Initialize this graph.
     */
    public GraphBase2() {
    }

    /**
     * Utility method: throw a ClosedException if this graph has been closed.
     */
    protected void checkOpen() {
        if (closed) {
            throw new ClosedException("already closed", this);
        }
    }

    /**
     * Close this graph. Subgraphs may extend to discard resources.
     */
    @Override
    public void close() {
        closed = true;
        if (reifier != null) {
            reifier.close();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * Default implementation answers {@code true} if this graph is the
     * same graph as the argument graph.
     */
    @Override
    public boolean dependsOn(final Graph other) {
        return this == other;
    }

    /**
     * @return a QueryHandler bound to this graph. The default implementation
     * returns the same SimpleQueryHandler each time it is called; sub-classes
     * may override if they need specialized query handlers.
     */
    public abstract QueryHandler queryHandler();

    /**
     * The query handler for this graph, or null if queryHandler() has not been
     * called yet.
     */
    protected QueryHandler queryHandler;

    @Override
    public GraphStatisticsHandler getStatisticsHandler() {
        if (statisticsHandler == null) {
            statisticsHandler = createStatisticsHandler();
        }
        return statisticsHandler;
    }

    protected GraphStatisticsHandler statisticsHandler;

    protected GraphStatisticsHandler createStatisticsHandler() {
        return null;
    }

    /**
     * @return the event manager for this graph; allocate a new one if required.
     * Subclasses may override if they have a more specialized event handler.
     * The default is a SimpleEventManager.
     */
    @Override
    public GraphEventManager getEventManager() {
        if (gem == null) {
            gem = new SimpleEventManager(this);
        }
        return gem;
    }

    /**
     * The event manager that this Graph uses to, well, manage events; allocated on
     * demand.
     */
    protected GraphEventManager gem;


    /**
     * Tell the event manager that the triple {@code t} has been added to the
     * graph.
     * @param t the {@link Triple}.
     */
    public void notifyAdd(final Triple t) {
        getEventManager().notifyAddTriple(this, t);
    }

    /**
     * Tell the event manager that the triple {@graph t} has been deleted from
     * the graph.
     * @param t the {@link Triple}.
     */
    public void notifyDelete(final Triple t) {
        getEventManager().notifyDeleteTriple(this, t);
    }

    /**
     * @return a transaction handler bound to this graph. The default is
     * SimpleTransactionHandler, which handles <i>no</i> transactions.
     */
    @Override
    public TransactionHandler getTransactionHandler() {
        return new SimpleTransactionHandler();
    }

    /**
     * @return a BulkUpdateHandler bound to this graph. The default is a
     * SimpleBulkUpdateHandler, which does bulk update by repeated simple
     * (add/delete) updates; the same handler is returned on each call. Subclasses
     * may override if they have specialized implementations.
     */
    public BulkUpdateHandler getBulkUpdateHandler() {
        if (bulkHandler == null) {
            bulkHandler = new SimpleBulkUpdateHandler(this);
        }
        return bulkHandler;
    }

    /**
     * The allocated BulkUpdateHandler, or null if no handler has been allocated yet.
     */
    protected BulkUpdateHandler bulkHandler;

    /**
     * @return the capabilities of this graph; the default is an AllCapabilities object
     * (the same one each time, not that it matters - Capabilities should be
     * immutable).
     */
    @Override
    public Capabilities getCapabilities() {
        if (capabilities == null) {
            capabilities = new AllCapabilities();
        }
        return capabilities;
    }

    /**
     * The allocated Capabilities object, or null if unallocated.
     */
    protected Capabilities capabilities = null;

    /**
     * @return the PrefixMapping object for this graph, the same one each time.
     * Subclasses are unlikely to want to modify this.
     */
    @Override
    public PrefixMapping getPrefixMapping() {
        if (pm == null) {
            pm = createPrefixMapping();
        }
        return pm;
    }

    private PrefixMapping pm = null;

    protected abstract PrefixMapping createPrefixMapping();

    /**
     * Add a triple, and notify the event manager. Subclasses should not need to
     * override this - we might make it final. The triple is added using performAdd,
     * and notification done by notifyAdd.
     */
    @Override
    public void add(final Triple t) {
        checkOpen();
        performAdd(t);
        notifyAdd(t);
    }

    /**
     * Add a triple to the triple store. The default implementation throws an
     * AddDeniedException; subclasses must override if they want to be able to
     * add triples.
     */
    @Override
    public void performAdd(final Triple t) {
        throw new AddDeniedException("GraphBase::performAdd");
    }

    /**
     * Delete a triple, and notify the event manager. Subclasses should not need to
     * override this - we might make it final. The triple is added using performDelete,
     * and notification done by notifyDelete.
     */
    @Override
    public final void delete(final Triple t) {
        checkOpen();
        performDelete(t);
        notifyDelete(t);
    }

    /**
     * Remove a triple from the triple store. The default implementation throws
     * a DeleteDeniedException; subclasses must override if they want to be able
     * to remove triples.
     */
    @Override
    public void performDelete(final Triple t) {
        throw new DeleteDeniedException("GraphBase::delete");
    }

    /**
     * @return an (extended) iterator over all the triples in this Graph matching
     * {@code m}. Subclasses cannot over-ride this, because it implements
     * the appending of reification quadlets; instead they must implement
     * {@link #graphBaseFind(Triple)}.
     */
    @Override
    public final ExtendedIterator<Triple> find(final Triple m) {
        checkOpen();
        //return reifierTriples(m).andThen(graphBaseFind(m));
        return graphBaseFind(m);
    }

    /**
     * @return an iterator over all the triples held in this graph's non-reified triple store
     * that match {@code m}. Subclasses <i>must</i> override; it is the core
     * implementation for {@link #find(Triple)}.
     */
    protected abstract ExtendedIterator<Triple> graphBaseFind(Triple m);

    public ExtendedIterator<Triple> forTestingOnly_graphBaseFind(final Triple tm) {
        return graphBaseFind(tm);
    }

    @Override
    public final ExtendedIterator<Triple> find(final Node s, final Node p, final Node o) {
        checkOpen();
        return graphBaseFind(s, p, o);
    }

    protected ExtendedIterator<Triple> graphBaseFind(final Node s, final Node p, final Node o) {
        return find(Triple.createMatch(s, p, o));
    }

    /**
     * @return {@code true} if {@code t} is in the graph as revealed by
     * {@link #find(t)} being non-empty. {@code t} may contain ANY
     * wildcards. Sub-classes may over-ride reifierContains and graphBaseContains
     * for efficiency.
     */
    @Override
    public final boolean contains(final Triple t) {
        checkOpen();
        //return reifierContains(t) || graphBaseContains(t);  }
        return graphBaseContains(t);
    }

    /**
     * @return {@code true} if the graph contains any triple matching {@code t}.
     * The default implementation uses {@code find} and checks to see
     * if the iterator is non-empty.
     */
    protected boolean graphBaseContains(final Triple t) {
        return containsByFind(t);
    }

    /**
     * @return {@code true} if this graph contains {@code (s, p, o)};
     * this canonical implementation cannot be over-ridden.
     */
    @Override
    public final boolean contains(final Node s, final Node p, final Node o) {
        checkOpen();
        return contains(Triple.create(s, p, o));
    }

    /**
     * Utility method: answer {@code true} if we can find at least one instantiation of
     * the triple in this graph using {@link #find(Triple)}.
     * @param t {@link Triple} that is the pattern to match
     * @return {@link true} if {@link #find(t)} returns at least one result
     */
    final protected boolean containsByFind(final Triple t) {
        final ClosableIterator<Triple> it = find(t);
        try {
            return it.hasNext();
        } finally {
            it.close();
        }
    }

    /**
     * @return this graph's reifier. The reifier may be lazily constructed, and it
     * must be the same reifier on each call. The default implementation is a
     * SimpleReifier. Generally DO NOT override this method: override
     * {@code constructReifier} instead.
     */
    public Reifier getReifier() {
        if (reifier == null) {
            reifier = constructReifier();
        }
        return reifier;
    }

    /**
     * @return a reifier appropriate to this graph. Subclasses override if
     * they need non-SimpleReifiers.
     */
    protected Reifier constructReifier() {
        return  new Reifier2(this);
    }

    /**
     * The cache variable for the allocated Reifier.
     */
    protected Reifier reifier = null;

    /**
     * @return the size of this graph (ie the number of exposed triples). Defined as
     * the size of the triple store plus the size of the reification store. Subclasses
     * must override graphBaseSize() to reimplement (and reifierSize if they have
     * some special reason for redefined that).
     */
    @Override
    public final int size() {
        checkOpen();
        return graphBaseSize();
    }

    /**
     * @return the number of triples in this graph. Default implementation counts its
     * way through the results of a findAll. Subclasses must override if they want
     * size() to be efficient.
     */
    protected int graphBaseSize() {
        final ExtendedIterator<Triple> it = GraphUtil.findAll(this);
        try {
            int tripleCount = 0;
            while (it.hasNext()) {
                it.next();
                tripleCount += 1;
            }
            return tripleCount;
        } finally {
            it.close();
        }
    }

    /**
     * @return {@code true} if this graph contains no triples (hidden reification quads do
     * not count). The default implementation is {@code size() == 0}, which is
     * fine if {@code size} is reasonable efficient. Subclasses may override
     * if necessary. This method may become final and defined in terms of other
     * methods.
     */
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * @return {@code true} if this graph is isomorphic to {@code g} according to
     * the algorithm (indeed, method) in {@code GraphMatcher}.
     */
    @Override
    public boolean isIsomorphicWith(final Graph g) {
        checkOpen();
        return g != null && GraphMatcher.equals(this, g);
    }

    /**
     * @return a human-consumable representation of this graph. Not advised for
     * big graphs, as it generates a big string: intended for debugging purposes.
     */
    @Override
    public String toString() {
        return GraphBase.toString("", this);
    }
}