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
package org.apache.rya.jena.jenasesame.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.GraphEvents;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.rya.jena.legacy.graph.BulkUpdateHandler;
import org.apache.rya.jena.legacy.graph.impl.SimpleBulkUpdateHandler;

/**
 * Bulk update handler.
 */
public class BulkUpdateHandlerNoIterRemove extends SimpleBulkUpdateHandler implements BulkUpdateHandler {
    /**
     * Creates a new instance of {@link BulkUpdateHandlerNoIterRemove}.
     * @param graph the {@link GraphWithPerform}. (not {@code null})
     */
    public BulkUpdateHandlerNoIterRemove(final GraphWithPerform graph) {
        super(checkNotNull(graph));
    }

    @Override
    public void remove(Node s, Node p, Node o) {
        s = fix(s);
        p = fix(p);
        o = fix(o);
        removeWorker(s, p, o);
        manager.notifyEvent(graph, GraphEvents.remove(s, p, o));
    }

    private static Node fix(final Node node) {
        return (node != null) ? node : Node.ANY;
    }

    @Override
    public void removeAll() {
         removeWorker(null, null, null);
         notifyRemoveAll();
    }

    private void removeWorker(final Node s, final Node p, final Node o) {
        ExtendedIterator<Triple> iter = null;
        try {
            iter = super.graph.find(s, p, o);
            final List<Triple> triples = Iter.toList(iter);

            for (final Triple triple : triples) {
                graph.performDelete(triple);
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
    }
}