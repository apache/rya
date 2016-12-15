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

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;

/**
 * A PatternStage is a Stage that handles some bunch of related patterns; those patterns
 * are encoded as Triples.
 */
public class PatternStage extends PatternStageBase {
    /**
     * Creates a new instance of {@link PatternStage}.
     * @param graph the {@link Graph}.
     * @param map the {@link Mapping}.
     * @param constraints the {@link ExpressionSet} of constraints.
     * @param triples the {@link Triple}s.
     */
    public PatternStage(final Graph graph, final Mapping map, final ExpressionSet constraints, final Triple[] triples) {
        super(QueryNode.FACTORY, graph, map, constraints, triples);
    }
}