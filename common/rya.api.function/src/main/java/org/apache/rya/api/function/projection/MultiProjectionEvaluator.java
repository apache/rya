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
package org.apache.rya.api.function.projection;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.BNodeGenerator;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.MultiProjection;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Processes a {@link MultiProjection} node from a SPARQL query.
 *
 * @see ProjectionEvaluator
 */
@DefaultAnnotation(NonNull.class)
public class MultiProjectionEvaluator {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private final Set<ProjectionEvaluator> projections;
    private final Set<String> blankNodeSourceNames;
    private final BNodeIdFactory bNodeIdFactory;

    /**
     * Constructs an instance of {@link MultiProjection}.
     *
     * @param projections - The {@link ProjectionEvaluators} that handle each projection within the MultiProjection. (not null)
     * @param blankNodeSourceNames - If there are blank nodes in the projection, this is a set of their names
     *   so that they may be re-labeled to have the same node IDs. (not null)
     * @param bNodeIdFactory - Creates the IDs for Blank Nodes. (not null)
     */
    public MultiProjectionEvaluator(
            final Set<ProjectionEvaluator> projections,
            final Set<String> blankNodeSourceNames,
            final BNodeIdFactory bnodeIdFactory) {
        this.projections = requireNonNull(projections);
        this.blankNodeSourceNames = requireNonNull(blankNodeSourceNames);
        this.bNodeIdFactory = requireNonNull(bnodeIdFactory);
    }

    /**
     * Make a {@link MultiProjectionEvaluator} that processes the logic of a {@link MultiProjection}.
     *
     * @param multiProjection - Defines the projections that will be processed. (not null)
     * @param bNodeIdFactory - Creates the IDs for Blank Nodes. (not null)
     * @return A {@link MultiProjectionEvaluator} for the provided {@link MultiProjection}.
     */
    public static MultiProjectionEvaluator make(final MultiProjection multiProjection, final BNodeIdFactory bNodeIdFactory) {
        requireNonNull(multiProjection);

        // Figure out if there are extensions.
        final TupleExpr arg = multiProjection.getArg();
        final Optional<Extension> extension = (arg instanceof Extension) ? Optional.of((Extension)arg): Optional.empty();

        // If there are, iterate through them and find any blank node source names.
        final Set<String> blankNodeSourceNames = new HashSet<>();
        if(extension.isPresent()) {
            for(final ExtensionElem elem : extension.get().getElements()) {
                if(elem.getExpr() instanceof BNodeGenerator) {
                    blankNodeSourceNames.add( elem.getName() );
                }
            }
        }

        // Create a ProjectionEvaluator for each projection that is part of the multi.
        final Set<ProjectionEvaluator> projections = new HashSet<>();
        for(final ProjectionElemList projectionElemList : multiProjection.getProjections()) {
            projections.add( new ProjectionEvaluator(projectionElemList, extension) );
        }

        return new MultiProjectionEvaluator(projections, blankNodeSourceNames, bNodeIdFactory);
    }

    /**
     * Apply the projections against a {@link VisibilityBindingSet}.
     *
     * @param bs - The value the projection will be applied to. (not null)
     * @return A set of values that result from the projection.
     */
    public Set<VisibilityBindingSet> project(final VisibilityBindingSet bs) {
        requireNonNull(bs);

        // Generate an ID for each blank node that will appear in the results.
        final Map<String, BNode> blankNodes = new HashMap<>();
        for(final String blankNodeSourceName : blankNodeSourceNames) {
            blankNodes.put(blankNodeSourceName, VF.createBNode(bNodeIdFactory.nextId()));
        }

        // Iterate through each of the projections and create the results from them.
        final Set<VisibilityBindingSet> results = new HashSet<>();
        for(final ProjectionEvaluator projection : projections) {
            results.add( projection.project(bs, blankNodes) );
        }

        return results;
    }
}