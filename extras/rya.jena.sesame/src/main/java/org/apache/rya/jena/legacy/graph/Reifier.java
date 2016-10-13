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
package org.apache.rya.jena.legacy.graph;

import org.apache.jena.graph.GetTriple;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.rya.jena.legacy.shared.ReificationStyle;

/**
 * This interface represents the type of things that can hold reified triples
 * for a Jena Graph.
 */
public interface Reifier extends GetTriple {
    /**
     * @param triple the {@link Triple}.
     * @return an iterator over all the reification triples in this Reifier that
     * match {@code triple}.
     */
    ExtendedIterator<Triple> find(Triple triple);

    /**
     * @param triple the {@link Triple}.
     * @return an iterator over all the reification triples that this Reifier
     * exposes (ie all if Standard, none otherwise) that match {@code triple}.
     */
    ExtendedIterator<Triple> findExposed(Triple triple);

    /**
     * @param triple the {@link Triple}.
     * @param showHidden {@code false} only uses the exposed triples. Otherwise
     * {@code true} only the concealed ones.
     * @return an iterator over the reification triples of this Reifier, or an
     * empty iterator.
     */
    ExtendedIterator<Triple> findEither(Triple triple, boolean showHidden);

    /**
     * @return the number of exposed reification quadlets held in this reifier.
     */
    int size();

    /**
     * @return this reifier's style.
     */
    ReificationStyle getStyle();

    /**
     * @return the Graph which uses this reifier.
     */
    Graph getParentGraph();

    /**
     * Note the triple {@code t} as reified using {@code n} as its representing
     * node.
     * If {@code n} is already reifying something, an AlreadyReifiedException is
     * thrown.
     * @param n the {@link Node}.
     * @param t the {@link Triple}.
     * @return the {@code Node}.
     */
    Node reifyAs(Node n, Triple t);

    /**
     * @param n the {@link Node}.
     * @return {@code true} if {@code n} is associated with some triple.
     */
    boolean hasTriple(Node n);

    /**
     * @param t the {@link Triple}
     * @return {@code true} if there are > 0 mappings to this triple.
     */
    boolean hasTriple(Triple t);

    /**
     * @return an iterator over all the nodes that are reifiying something in
     * this reifier.
     */
    ExtendedIterator<Node> allNodes();

    /**
     * @param t the {@link Triple}.
     * @return an iterator over all the nodes that are reifiying {@code t} in
     * this reifier.
     */
    ExtendedIterator<Node> allNodes(Triple t);

    /**
     * Remove any existing binding for {@code n}; hasNode(n) will return
     * {@code false} and {@link #getTriple(n)} will return {@code null}. This only
     * removes *unique, single* bindings.
     * @param n the {@link Node}.
     * @param t the {@link Triple}.
     */
    void remove(Node n, Triple t);

    /**
     * Remove all bindings which map to this triple.
     * @param t the {@link Triple}.
     */
    void remove(Triple t);

    /**
     * @param t the {@link Triple}.
     * @return {@code true} if the Reifier has handled an add of the triple
     * {@code t}.
     */
    boolean handledAdd(Triple t);

    /**
     * @param t the {@link Triple}.
     * @return {@code true} if the Reifier has handled a remove of the triple
     * {@code t}.
     */
    boolean handledRemove(Triple t);

    /**
     * The reifier will no longer be used. Further operations on it are not
     * defined by this interface.
     */
    void close();

    /**
     * Reifier utility methods.
     */
    public static class Util {
        /**
         * Private constructor to prevent instantiation.
         */
        private Util() {
        }

        /**
         * @param node the {@link Node}.
         * @return {@code true} if the node is a reification predicate.
         * {@code false} otherwise.
         */
        public static boolean isReificationPredicate(final Node node) {
            return node.equals(RDF.Nodes.subject) || node.equals(RDF.Nodes.predicate) || node.equals(RDF.Nodes.object);
        }

        /**
         * @param node the {@link Node}.
         * @return {@code true} if the node could be a statement. {@code false}
         * otherwise.
         */
        public static boolean couldBeStatement(final Node node) {
            return node.isVariable() || node.equals(Node.ANY) || node.equals(RDF.Nodes.Statement);
        }

        /**
         * @param p the predicate {@link Node}.
         * @param o the object {@link Node}.
         * @return {@code true} if the nodes are a reification type.
         * {@code false} otherwise.
         */
        public static boolean isReificationType(final Node p, final Node o) {
            return p.equals(RDF.Nodes.type) && couldBeStatement(o);
        }
    }
}