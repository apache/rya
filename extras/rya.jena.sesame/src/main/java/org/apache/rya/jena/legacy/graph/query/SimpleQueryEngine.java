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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.log4j.Logger;

/**
 * SimpleQueryEngine
 */
public class SimpleQueryEngine {
    private static final Logger log = Logger.getLogger(SimpleQueryEngine.class);

    private final ExpressionSet constraint;
    private final NamedTripleBunches triples;
    private final TripleSorter sortMethod;
    private int variableCount;

    /**
     * @deprecated NamedTripleBunches are not supported. Use SimpleQueryEngine
     * (List, TripleSorter, ExpressionSet) instead.
     */
    @Deprecated
    public SimpleQueryEngine(final NamedTripleBunches triples, final TripleSorter ts, final ExpressionSet constraint) {
        this.constraint = constraint;
        this.triples = triples;
        this.sortMethod = ts;
    }

    public SimpleQueryEngine(final List<Triple> pattern, final TripleSorter sorter, final ExpressionSet constraints) {
        this.constraint = constraints;
        this.triples = asNamedTripleBunches(pattern);
        this.sortMethod = sorter;
    }

    private static NamedTripleBunches asNamedTripleBunches(final List<Triple> pattern) {
        final NamedTripleBunches result = new NamedTripleBunches();
        for (final Triple triple : pattern) {
            result.add(NamedTripleBunches.anon, triple);
        }
        return result;
    }

    int getVariableCount() {
        return variableCount;
    }

    public ExtendedIterator<Domain> executeBindings(final List<Stage> outStages, final NamedGraphMap args, final Node[] nodes) {
        final Mapping map = new Mapping(nodes);
        final ArrayList<Stage> stages = new ArrayList<Stage>();
        addStages(stages, args, map);
        if (constraint.isComplex()) {
            stages.add(new ConstraintStage(map, constraint));
        }
        outStages.addAll(stages);
        variableCount = map.size();
        return filter(connectStages(stages, variableCount));
    }

    private ExtendedIterator<Domain> filter(final Stage allStages) {
        // final Pipe complete = allStages.deliver(new BufferPipe());
        return new NiceIterator<Domain>() {
            private Pipe complete;

            private void ensurePipe() {
                if (complete == null) {
                    complete = allStages.deliver(new BufferPipe());
                }
            }

            @Override
            public void close() {
                allStages.close();
                clearPipe();
            }

            @Override
            public Domain next() {
                ensurePipe();
                return complete.get();
            }

            @Override
            public boolean hasNext() {
                ensurePipe();
                return complete.hasNext();
            }

            private void clearPipe() {
                int count = 0;
                while (hasNext()) {
                    count += 1;
                    next();
                }
                log.trace("The pipe cleared out " + count + " items.");
            }
        };
    }

    public static Cons cons(final Triple pattern, final Object cons) {
        return new Cons(pattern, (Cons) cons);
    }

    public static class Cons {
        Triple head;
        Cons tail;

        /**
         * Creates a new instance of {@link Cons}.
         * @param head the {@link Triple} head.
         * @param tail the {@link Cons} tail.
         */
        Cons(final Triple head, final Cons tail) {
            this.head = head;
            this.tail = tail;
        }

        static int size(Cons L) {
            int n = 0;
            while (L != null) {
                n += 1;
                L = L.tail;
            }
            return n;
        }
    }

    private void addStages(final ArrayList<Stage> stages, final NamedGraphMap arguments, final Mapping map) {
        final Iterator<Map.Entry<String, Cons>> it2 = triples.entrySetIterator();
        while (it2.hasNext()) {
            final Map.Entry<String, Cons> e = it2.next();
            final String name = e.getKey();
            Cons nodeTriples = e.getValue();
            final Graph g = arguments.get(name);
            final int nBlocks = Cons.size(nodeTriples);
            int i = nBlocks;
            Triple[] nodes = new Triple[nBlocks];
            while (nodeTriples != null) {
                nodes[--i] = nodeTriples.head;
                nodeTriples = nodeTriples.tail;
            }
            nodes = sortTriples(nodes);
            final Stage next = SimpleQueryHandler.findOrCreate(g).patternStage(map, constraint, nodes);
            stages.add(next);
        }
    }

    private Triple[] sortTriples(final Triple[] ts) {
        return sortMethod.sort(ts);
    }

    private Stage connectStages(final ArrayList<Stage> stages, final int count) {
        Stage current = Stage.initial(count);
        for (int i = 0; i < stages.size(); i += 1) {
            current = stages.get(i).connectFrom(current);
        }
        return current;
    }
}