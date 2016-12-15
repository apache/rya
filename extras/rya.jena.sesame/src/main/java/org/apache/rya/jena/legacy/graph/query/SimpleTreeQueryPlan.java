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
import java.util.Iterator;
import java.util.Set;

import org.apache.jena.graph.Factory;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphUtil;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.CollectionFactory;
import org.apache.jena.util.iterator.ClosableIterator;

/**
 * Incomplete class. Do not use.
 */
public class SimpleTreeQueryPlan implements TreeQueryPlan {
    private final Graph pattern;
    private final Graph target;

    public SimpleTreeQueryPlan(final Graph target, final Graph pattern) {
        this.target = target;
        this.pattern = pattern;
    }

    @Override
    public Graph executeTree() {
        final Graph result = Factory.createGraphMem();
        final Set<Node> roots = getRoots(pattern);
        for (final Iterator<Node> it = roots.iterator(); it.hasNext(); handleRoot(result, it.next(), new HashSet<Triple>())) {
        }
        return result;
    }

    private Iterator<Triple> findFromTriple(final Graph g, final Triple t) {
        return g.find(asPattern(t.getSubject()), asPattern(t.getPredicate()), asPattern(t.getObject()));
    }

    private Node asPattern(final Node node) {
        return node.isBlank() ? null : node;
    }

    private void handleRoot(final Graph result, final Node root, final Set<Triple> pending) {
        final ClosableIterator<Triple> it = pattern.find(root, null, null);
        if (!it.hasNext()) {
            absorb(result, pending);
            return;
        }
        while (it.hasNext()) {
            final Triple base = it.next();
            final Iterator<Triple> that = findFromTriple(target, base);
            while (that.hasNext()) {
                final Triple triple = that.next();
                pending.add(triple);
                handleRoot(result, base.getObject(), pending);
            }
        }
    }

    private void absorb(final Graph result, final Set<Triple> triples) {
        for (final Iterator<Triple> it = triples.iterator(); it.hasNext(); result.add(it.next())) {
        }
        triples.clear();
    }

    public static Set<Node> getRoots(final Graph pattern) {
        final Set<Node> roots = CollectionFactory.createHashedSet();
        final ClosableIterator<Triple> sub = GraphUtil.findAll(pattern);
        while (sub.hasNext()) {
            roots.add(sub.next().getSubject());
        }
        final ClosableIterator<Triple> obj = GraphUtil.findAll(pattern);
        while (obj.hasNext()) {
            roots.remove(obj.next().getObject());
        }
        return roots;
    }
}