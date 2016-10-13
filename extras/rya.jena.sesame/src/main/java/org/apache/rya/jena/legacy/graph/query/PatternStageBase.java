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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.jena.JenaRuntime;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.rya.jena.legacy.graph.query.StageElement.PutBindings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PatternStageBase contains the features that are common to the
 * traditional PatternStage engine and the Faster engine. (Eventually
 * the two will merge back together.) Notable, it:
 *
 * <ul>
 *   <li>remembers the graph
 *   <li>classifies all the triples according to the factory
 *   <li>constructs the array of applicable guards
 * </ul>
 */
public abstract class PatternStageBase extends Stage {
    protected static int count = 0;
    protected final ValuatorSet[] guards;
    protected final QueryTriple[] classified;
    protected final Graph graph;
    protected final QueryNodeFactory factory;

    /**
     * Creates a new instance of {@link PatternStageBase}.
     * @param factory the {@link QueryNodeFactory}.
     * @param graph the {@link Graph}.
     * @param map the {@link Mapping}.
     * @param constraints the {@link ExpressionSet} of constraints.
     * @param triples the {@link Triple}s.
     */
    public PatternStageBase(final QueryNodeFactory factory, final Graph graph, final Mapping map, final ExpressionSet constraints, final Triple[] triples) {
        this.graph = graph;
        this.factory = factory;
        this.classified = QueryTriple.classify(factory, map, triples);
        this.guards = new GuardArranger(triples).makeGuards(map, constraints);
    }

    static Logger log = LoggerFactory.getLogger(PatternStageBase.class);

    protected void run(final Pipe source, final Pipe sink, final StageElement se) {
        try {
            while (stillOpen && source.hasNext()) {
                se.run(source.get());
            }
        } catch (final Exception e) {
            log.debug("PatternStageBase has caught and forwarded an exception", e);
            sink.close(e);
            return;
        }
        sink.close();
    }

    private final class PatternStageThread extends Thread {
        private final BlockingQueue<Work> buffer = new ArrayBlockingQueue<Work>(1);

        /**
         * Creates a new instance of {@link PatternStageThread}.
         * @param name the name of the thread.
         */
        public PatternStageThread(final String name) {
            super(name);
        }

        public void put(final Work w) {
            try {
                buffer.put(w);
            } catch (final InterruptedException e) {
                throw new BufferPipe.BoundedBufferPutException(e);
            }
        }

        protected Work get() {
            try {
                return buffer.take();
            } catch (final InterruptedException e) {
                throw new BufferPipe.BoundedBufferTakeException(e);
            }
        }

        @Override
        public void run() {
            while (true) {
                get().run();
                addToAvailableThreads(this);
            }
        }
    }

    public class Work {
        protected final Pipe source;
        protected final Pipe sink;
        protected final StageElement e;

        public Work(final Pipe source, final Pipe sink, final StageElement e) {
            this.source = source;
            this.sink = sink;
            this.e = e;
        }

        public void run() {
            PatternStageBase.this.run(source, sink, e);
        }
    }

    public static boolean reuseThreads = JenaRuntime.getSystemProperty("jena.reusepatternstage.threads", "yes").equals("yes");

    @Override
    public synchronized Pipe deliver(final Pipe sink) {
        final Pipe source = previous.deliver(new BufferPipe());
        final StageElement s = makeStageElementChain(sink, 0);
        if (reuseThreads) {
            getAvailableThread().put(new Work(source, sink, s));
        } else {
            final Thread thread = new Thread("PatternStage-" + ++count) {
                @Override
                public void run() {
                    PatternStageBase.this.run(source, sink, s);
                }
            };
            thread.start();
        }
        return sink;
    }

    private static final List<PatternStageThread> threads = new ArrayList<PatternStageThread>();

    private void addToAvailableThreads(final PatternStageThread thread) {
        synchronized (threads) {
            threads.add(thread);
            log.debug("caching thread " + this + " [currently " + threads.size() + " cached threads]");
        }
    }

    private PatternStageThread getAvailableThread() {
        synchronized (threads) {
            final int size = threads.size();
            if (size > 0) {
                final PatternStageThread thread = threads.remove(size - 1);
                log.debug("reusing thread " + thread);
                return thread;
            }
        }
        final PatternStageThread patternStageThread = new PatternStageThread("PatternStage-" + ++count);
        log.debug("created new thread " + patternStageThread);
        patternStageThread.setDaemon(true);
        patternStageThread.start();
        return patternStageThread;
    }

    protected StageElement makeStageElementChain(final Pipe sink, final int index) {
        return index < classified.length ? makeIntermediateStageElement(sink, index) : makeFinalStageElement(sink);
    }

    protected PutBindings makeFinalStageElement(final Pipe sink) {
        return new StageElement.PutBindings(sink);
    }

    protected StageElement makeIntermediateStageElement(final Pipe sink, final int index) {
        final StageElement next = makeNextStageElement(sink, index);
        return makeFindStageElement(index, next);
    }

    protected StageElement makeNextStageElement(final Pipe sink, final int index) {
        final ValuatorSet s = guards[index];
        final StageElement rest = makeStageElementChain(sink, index + 1);
        return s.isNonTrivial() ? new StageElement.RunValuatorSet(s, rest) : rest;
    }

    protected StageElement makeFindStageElement(final int index, final StageElement next) {
        final Applyer f = classified[index].createApplyer(graph);
        final Matcher m = classified[index].createMatcher();
        return new StageElement.FindTriples(this, m, f, next);
    }
}