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
 * a processing stage in the query pipeline. Each stage
 * gets connected to its predecessor in the pipeline, and
 * mangles the contents before handing them on to the next
 * stage.
 */
public abstract class Stage {
    /**
     * the previous stage of the pipeline, once connected
     */
    protected Stage previous;

    public volatile boolean stillOpen = true;

    /**
     * construct a new initial stage for the pipeline
     */
    public static Stage initial(final int count) {
        return new InitialStage(count);
    }

    /**
     * connect this stage to its supplier;
     * @return this for chaining.
     */
    public Stage connectFrom(final Stage s) {
        previous = s;
        return this;
    }

    public boolean isClosed() {
        return !stillOpen;
    }

    protected final void markClosed() {
        stillOpen = false;
    }

    public void close() {
        previous.close();
        markClosed();
    }

    /**
     * Execute the pipeline and pump the results into {@code sink}; this is
     * asynchronous. Deliver that same {@code sink} as our result. (This allows
     * the sink to be created as the argument to {@link #deliver}.
     */
    public abstract Pipe deliver(Pipe sink);
}