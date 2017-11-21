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
import java.util.UUID;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.openrdf.model.BNode;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.BNodeGenerator;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.impl.MapBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Processes a {@link Projection} node from a SPARQL query.
 * </p>
 * A projection is used transform the bindings that are in a Binding Set. It may do the following things:
 * <ul>
 *   <li>Change the binding name for a value.</li>
 *   <li>Completely remove a binding from the Binding Set.</li>
 *   <li>Insert a binding that has a constant value.</li>
 *   <li>Insert a binding with a blank node that identifies some resource.</li>
 * </ul>
 * </p>
 * If you do not supply ID values for the blank nodes that may result from a projection, then a random {@link UUID}
 * is used.
 */
@DefaultAnnotation(NonNull.class)
public class ProjectionEvaluator {

    private final ValueFactory vf = new ValueFactoryImpl();

    /**
     * All off the projection elements that define what will appear in the resulting binding sets.
     */
    private final ProjectionElemList projectionElems;

    /**
     * Maps from a ProjectionElem's source name to the constant value that should be used for that name
     * in resulting binding sets.
     */
    private final Map<String, Value> constantSources = new HashMap<>();

    /**
     * A set of names for the anonymous source names. These values will be blank node UUIDs.
     */
    private final Set<String> anonymousSources = new HashSet<>();

    /**
     * Constructs an instance of {@link ProjectionEvaluator}.
     *
     * @param projectionElems - Defines the structure of the resulting value. (not null)
     * @param extensions - Extra information about the projection elements when there are anonymous constants or blank
     *   nodes within the projection elements. (not null)
     */
    public ProjectionEvaluator(final ProjectionElemList projectionElems, final Optional<Extension> extensions) {
        this.projectionElems = requireNonNull(projectionElems);
        requireNonNull(extensions);

        // Find all extensions that represent constant insertions.
        if(extensions.isPresent()) {
            for(final ExtensionElem extensionElem : extensions.get().getElements()) {
                final ValueExpr valueExpr = extensionElem.getExpr();

                // If the extension is a ValueConstant, store it so that they may be added to the binding sets.
                if(valueExpr instanceof ValueConstant) {
                    final String sourceName = extensionElem.getName();
                    final Value targetValue = ((ValueConstant) valueExpr).getValue();
                    constantSources.put(sourceName, targetValue);
                }

                // If the extension is a BNodeGenerator, keep track of the name so that we know we have to generate an ID for it.
                else if(valueExpr instanceof BNodeGenerator) {
                    final String sourceName = extensionElem.getName();
                    anonymousSources.add( sourceName );
                }
            }
        }
    }

    /**
     * Make a {@link ProjectionEvaluator} that processes the logic of a {@link Projection}.
     *
     * @param projection - Defines the projection that will be processed. (not null)
     * @return A {@link ProjectionEvaluator} for the provided {@link Projection}.
     */
    public static ProjectionEvaluator make(final Projection projection) {
        requireNonNull(projection);

        final ProjectionElemList projectionElems = projection.getProjectionElemList();

        final TupleExpr arg = projection.getArg();
        final Optional<Extension> extension = arg instanceof Extension ? Optional.of((Extension)arg) : Optional.empty();

        return new ProjectionEvaluator(projectionElems, extension);
    }

    /**
     * Applies the projection to a value. If the result has any blank nodes, those nodes will use random UUIDs.
     * If you want to control what those IDs are, then use {@link #project(VisibilityBindingSet, Map)} instead.
     *
     * @param bs - The value the projection will be applied to. (not null)
     * @return A new value that is the result of the projection.
     */
    public VisibilityBindingSet project(final VisibilityBindingSet bs) {
        return project(bs, new HashMap<>());
    }

    /**
     * Applies the projection to a value. If the result has a blank node whose ID is not mapped to a value in
     * {@code blankNodes}, then a random UUID will be used.
     *
     * @param bs - The value the projection will be applied to. (not null)
     * @param blankNodes - A map from node source names to the blank nodes that will be used for those names. (not null)
     * @return A new value that is the result of the projection.
     */
    public VisibilityBindingSet project(final VisibilityBindingSet bs, final Map<String, BNode> blankNodes) {
        requireNonNull(bs);
        requireNonNull(blankNodes);

        // Apply the projection elements against the original binding set.
        final MapBindingSet result = new MapBindingSet();
        for (final ProjectionElem elem : projectionElems.getElements()) {
            final String sourceName = elem.getSourceName();

            Value value = null;

            // If the binding set already has the source name, then use the target name.
            if (bs.hasBinding(sourceName)) {
                value = bs.getValue(elem.getSourceName());
            }

            // If the source name represents a constant value, then use the constant.
            else if(constantSources.containsKey(sourceName)) {
                value = constantSources.get(sourceName);
            }

            // If the source name represents an anonymous value, then create a Blank Node.
            else if(anonymousSources.contains(sourceName)) {
                if(blankNodes.containsKey(sourceName)) {
                    value = blankNodes.get(sourceName);
                } else {
                    value = vf.createBNode( UUID.randomUUID().toString() );
                }
            }

            // Only add the value if there is one. There may not be one if a binding is optional.
            if(value != null) {
                result.addBinding(elem.getTargetName(), value);
            }
        }

        return new VisibilityBindingSet(result, bs.getVisibility());
    }
}