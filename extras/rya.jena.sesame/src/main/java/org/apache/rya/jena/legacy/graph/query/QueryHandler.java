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
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 * a QueryHandler handles queries on behalf of a graph. Its primary purpose
 * is to isolate changes to the query interface away from the Graph; multiple
 * different Graph implementations can use the same QueryHandler class, such
 * as the built-in SimpleQueryHandler.
 */
public interface QueryHandler {
    /**
     * Prepare a plan for generating bindings given the query {@code q} and the
     * result variables {@code variables}.
     * @param q the {@link GraphQuery}.
     * @param variables the array of {@link Node} variables.
     * @return the {@link BindingQueryPlan}.
     */
    public BindingQueryPlan prepareBindings(GraphQuery q, Node[] variables);

    /**
     * Produce a single Stage which will probe the underlying graph for triples
     * matching {@code triples} and inject all the resulting bindings into the
     * processing stream (see Stage for details)
     * <p>
     * @param map the variable binding map to use and update.
     * @param constraints the current constraint expression: if this Stage can
     * absorb some of the ANDed constraints, it may do so, and remove them from
     * the ExpressionSet.
     * @param triples the array of {@link Triple}s.
     * @return the {@link Stage}.
     */
    public Stage patternStage(Mapping map, ExpressionSet constraints, Triple[] triples);

    /**
     * Deliver a plan for executing the tree-match query defined by
     * {@code pattern}.
     * @param pattern the {@link Graph}.
     * @return the {@link TreeQueryPlan}.
     */
    public TreeQueryPlan prepareTree(Graph pattern);

    /**
     * Deliver an iterator over all the objects {@code o} such that
     * {@code (s, p, o)} is in the underlying graph; {@code null}s count as
     * wildcards. .remove() is not defined on this iterator.
     * @param s the subject {@link Node}.
     * @param p the predicate {@link Node}.
     * @return the {@link ExtendedIterator} of {@link Node}s.
     */
    public ExtendedIterator<Node> objectsFor(Node s, Node p);

    /**
     * Deliver an iterator over all the subjects {@code s} such that
     * {@code (s, p, o)} is in the underlying graph; nulls count as wildcards.
     * .remove() is not defined on this iterator.
     * @param p the predicate {@link Node}.
     * @param o the object {@link Node}.
     * @return the {@link ExtendedIterator} of {@link Node}s
     */
    public ExtendedIterator<Node> subjectsFor(Node p, Node o);

    /**
     * Deliver an iterator over all the predicates {@code p} such that
     * {@code (s, p, o)} is in the underlying graph. .remove() is not defined on
     * this iterator.
     * @param s the subject {@link Node}.
     * @param o the object {@link Node}.
     * @return the {@link ExtendedIterator} of {@link Node}s.
     */
    public ExtendedIterator<Node> predicatesFor(Node s, Node o);

    /**
     * @param n the {@link Node}.
     * @return {@code true} if the graph contains a triple in which {@code n}
     * appears somewhere. If {@code n} is a fluid node, it is not define
     * whether {@code true} or {@code false} is returned, so don't do that.
     */
    public boolean containsNode(Node n);
}