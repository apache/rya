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

import java.util.Map;

import org.apache.jena.graph.Graph;
import org.apache.jena.util.CollectionFactory;

/**
 * A mapping from from names to Graphs.
 */
public class NamedGraphMap {
    /**
     * Creates a new instance of {@link NamedGraphMap}.
     */
    NamedGraphMap() {
    }

    private final Map<String, Graph> map = CollectionFactory.createHashedMap();

    /**
     * Add a named graph to the map and return this map.
     * @param name the name to give this graph. Must not already be bound.
     * @param g the graph to name
     * @return this NamedGraphMap
     */
    public NamedGraphMap put(final String name, final Graph g) {
        map.put(name, g);
        return this;
    }

    /**
     * @param name the name of the graph
     * @return the Graph with the given name, or null if there isn't one.
     */
    public Graph get(final String name) {
        return map.get(name);
    }
}