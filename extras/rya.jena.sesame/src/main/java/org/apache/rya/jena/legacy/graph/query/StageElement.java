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

/**
 * Class used internally by PatternStage to express the notion of "the
 * runnable next component in this stage".
 */
public abstract class StageElement {
    public abstract void run(Domain current);

    /**
     * A PutBindings is created with a domain sink and, whenever it is run,
     * puts a copy of the current domain down the sink.
     */
    public static final class PutBindings extends StageElement {
        protected final Pipe sink;

        public PutBindings(final Pipe sink) {
            this.sink = sink;
        }

        @Override
        public final void run(final Domain current) {
            sink.put(current.copy());
        }
    }

    /**
     * A FindTriples runs match-and-next over all the triples returned
     * by its finder.
     */
    public static final class FindTriples extends StageElement {
        protected final Matcher matcher;
        protected final Applyer finder;
        protected final StageElement next;
        protected final Stage stage;

        public FindTriples(final Stage stage, final Matcher matcher, final Applyer finder, final StageElement next) {
            this.stage = stage;
            this.matcher = matcher;
            this.finder = finder;
            this.next = next;
        }

        @Override
        public final void run(final Domain current) {
            if (stage.stillOpen) {
                finder.applyToTriples(current, matcher, next);
            }
        }
    }

    /**
     * A RunValuatorSet is created with a ValuatorSet and a next StageElement;
     * whenever it is run, it evaluates the ValuatorSet and only if that
     * answers {@code true} does it run the next StageElement.
     */
    public static final class RunValuatorSet extends StageElement {
        protected final ValuatorSet s;
        protected final StageElement next;

        /**
         * Creates a new instance of {@link RunValuatorSet}.
         * @param s the {@link ValuatorSet}.
         * @param next the {@link StageElement}.
         */
        public RunValuatorSet(final ValuatorSet s, final StageElement next) {
            this.s = s;
            this.next = next;
        }

        @Override
        public final void run(final Domain current) {
            if (s.evalBool(current)) {
                next.run(current);
            }
        }
    }
}