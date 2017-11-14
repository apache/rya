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
package org.apache.rya.streams.kafka.topology;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Factory for building {@link TopologyBuilder}s from a SPARQL query.
 */
@DefaultAnnotation(NonNull.class)
public interface TopologyBuilderFactory {

    /**
     * Builds a {@link TopologyBuilder} based on the provided sparql query where
     * each {@link TupleExpr} in the parsed query is a processor in the
     * topology.
     *
     * @param sparqlQuery - The SPARQL query to build a topology for. (not null)
     * @param statementTopic - The topic for the source to read from. (not null)
     * @param statementTopic - The topic for the sink to write to. (not null)
     * @return - The created {@link TopologyBuilder}.
     * @throws MalformedQueryException - The provided query is not a valid
     *         SPARQL query.
     */
    public TopologyBuilder build(final String sparqlQuery, final String statementTopic, final String resultTopic)
            throws Exception;
}
