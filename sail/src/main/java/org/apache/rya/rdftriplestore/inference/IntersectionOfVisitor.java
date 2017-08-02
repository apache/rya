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
package org.apache.rya.rdftriplestore.inference;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.Var;

/**
 * Visitor for handling owl:intersectionOf inferencing on a node.
 */
public class IntersectionOfVisitor extends AbstractInferVisitor {
    private static final Logger log = Logger.getLogger(IntersectionOfVisitor.class);

    /**
     * Creates a new instance of {@link IntersectionOfVisitor}.
     * @param conf the {@link RdfCloudeTripleStoreConfiguration}.
     * @param inferenceEngine the {@link InferenceEngine}.
     */
    public IntersectionOfVisitor(final RdfCloudTripleStoreConfiguration conf, final InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = conf.isInferIntersectionOf();
    }

    @Override
    protected void meetSP(final StatementPattern node) throws Exception {
        final StatementPattern currentNode = node.clone();
        final Var subVar = node.getSubjectVar();
        final Var predVar = node.getPredicateVar();
        final Var objVar = node.getObjectVar();
        final Var conVar = node.getContextVar();
        if (predVar != null && objVar != null && objVar.getValue() != null && RDF.TYPE.equals(predVar.getValue()) && !EXPANDED.equals(conVar)) {
            final List<Set<Resource>> intersections = inferenceEngine.getIntersectionsImplying((URI) objVar.getValue());
            if (intersections != null && !intersections.isEmpty()) {
                final List<TupleExpr> joins = new ArrayList<>();
                for (final Set<Resource> intersection : intersections) {
                    final Set<Resource> sortedIntersection = new TreeSet<>(new ResourceComparator());
                    sortedIntersection.addAll(intersection);

                    // Create a join tree of all statement patterns in the
                    // current intersection.
                    final TupleExpr joinTree = createJoinTree(new ArrayList<>(sortedIntersection), subVar, conVar);
                    if (joinTree != null) {
                        joins.add(joinTree);
                    }
                }

                if (!joins.isEmpty()) {
                    // Combine all the intersection join trees for the type
                    // together into a union tree.  This will be a join tree if
                    // only one intersection exists.
                    final TupleExpr unionTree = createUnionTree(joins);
                    // Union the above union tree of intersections with the
                    // original node.
                    final Union union = new InferUnion(unionTree, currentNode);
                    node.replaceWith(union);
                    log.trace("Replacing node with inferred intersection union: " + union);
                }
            }
        }
    }

    /**
     * Recursively creates a {@link TupleExpr} tree comprised of
     * {@link InferJoin}s and {@link StatementPattern}s. The left arg is a
     * {@link StatementPattern} and the right arg is either a
     * {@link StatementPattern} if it's the final element or a nested
     * {@link InferJoin}.<p>
     * A list of {@code [:A, :B, :C, :D, :E]} with type {@code ?x} returns:
     * <pre>
     * InferJoin(
     *     StatementPattern(?x, rdf:type, :A),
     *     InferJoin(
     *         StatementPattern(?x, rdf:type, :B),
     *         InferJoin(
     *             StatementPattern(?x, rdf:type, :C),
     *             InferJoin(
     *                 StatementPattern(?x, rdf:type, :D),
     *                 StatementPattern(?x, rdf:type, :E)
     *             )
     *         )
     *     )
     * )
     * </pre>
     * @param intersection a {@link List} of {@link Resource}s.
     * @param typeVar the type {@link Var} to use as the subject for the
     * {@link StatementPattern}.
     * @param conVar the {@link Var} to use as the context for the
     * {@link StatementPattern}.
     * @return the {@link TupleExpr} tree. Returns {@code null} if
     * {@code intersection} is empty. Returns a {@link StatementPattern} if
     * {@code intersection}'s size is 1.  Otherwise, returns an
     * {@link InferJoin} which may contain more nested {@link InferJoin}s.
     */
    private static TupleExpr createJoinTree(final List<Resource> intersection, final Var typeVar, final Var conVar) {
        if (intersection.isEmpty()) {
            return null;
        } else {
            final Var predVar = new Var(RDF.TYPE.toString(), RDF.TYPE);
            final Resource resource = intersection.get(0);
            final Var valueVar = new Var(resource.toString(), resource);
            final StatementPattern left = new StatementPattern(typeVar, predVar, valueVar, conVar);
            if (intersection.size() == 1) {
                return left;
            } else {
                final List<Resource> subList = intersection.subList(1, intersection.size());
                final TupleExpr right = createJoinTree(subList, typeVar, conVar);
                final InferJoin join = new InferJoin(left, right);
                join.getProperties().put(InferConstants.INFERRED, InferConstants.TRUE);
                return join;
            }
        }
    }

    /**
     * Recursively creates a {@link TupleExpr} tree comprised of
     * {@link InferUnion}s and {@link InferJoin}s. The left arg is a
     * {@link InferJoin} and the right arg is either a {@link InferJoin} if it's
     * the final element or a nested {@link InferUnion}.<p>
     * A list of {@code [JoinA, JoinB, JoinC, JoinD, JoinE]} returns:
     * <pre>
     * InferUnion(
     *     JoinA,
     *     InferUnion(
     *         JoinB,
     *         InferUnion(
     *             JoinC,
     *             InferUnion(
     *                 JoinD,
     *                 JoinE
     *             )
     *         )
     *     )
     * )
     * </pre>
     * @param joins a {@link List} of {@link TupleExpr}s that is most likely
     * comprised of {@link Join}s but may be a single {@link StatementPattern}.
     * @return the {@link TupleExpr} tree. Returns {@code null} if
     * {@code joins} is empty. Might return a {@link StatementPattern} if
     * {@code joins}' size is 1.  Otherwise, returns an
     * {@link InferUnion} which may contain more nested {@link InferUnion}s.
     */
    private static TupleExpr createUnionTree(final List<TupleExpr> joins) {
        if (joins.isEmpty()) {
            return null;
        } else {
            final TupleExpr left = joins.get(0);
            if (joins.size() == 1) {
                return left;
            } else {
                final List<TupleExpr> subList = joins.subList(1, joins.size());
                final TupleExpr right = createUnionTree(subList);
                return new InferUnion(left, right);
            }
        }
    }

    /**
     * Sorts {@link Resource}s in ascending order based on their string values.
     */
    public static class ResourceComparator implements Comparator<Resource> {
        private static final Comparator<String> NULL_SAFE_STRING_COMPARATOR =
            Comparator.nullsFirst(String::compareTo);

        private static final Comparator<Resource> RESOURCE_COMPARATOR =
            Comparator.comparing(Resource::stringValue, NULL_SAFE_STRING_COMPARATOR);

        @Override
        public int compare(final Resource r1, final Resource r2) {
            if (r1 == null && r2 != null) {
                return -1;
            }
            if (r1 != null && r2 == null) {
                return 1;
            }
            if (r1 == null && r2 == null) {
                return 0;
            }
            return RESOURCE_COMPARATOR.compare(r1, r2);
        }
    }
}