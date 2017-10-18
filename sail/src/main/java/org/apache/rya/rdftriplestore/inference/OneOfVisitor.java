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

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

/**
 * Visitor for handling owl:oneOf inferencing on a node.
 */
public class OneOfVisitor extends AbstractInferVisitor {
    private static final Logger log = Logger.getLogger(OneOfVisitor.class);

    /**
     * Creates a new instance of {@link OneOfVisitor}.
     * @param conf the {@link RdfCloudeTripleStoreConfiguration}.
     * @param inferenceEngine the {@link InferenceEngine}.
     */
    public OneOfVisitor(final RdfCloudTripleStoreConfiguration conf, final InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = conf.isInferOneOf();
    }

    @Override
    protected void meetSP(final StatementPattern node) throws Exception {
        final Var subVar = node.getSubjectVar();
        final Var predVar = node.getPredicateVar();
        final Var objVar = node.getObjectVar();
        final Var conVar = node.getContextVar();
        if (predVar != null && objVar != null && objVar.getValue() != null && objVar.getValue() instanceof Resource && RDF.TYPE.equals(predVar.getValue()) && !EXPANDED.equals(conVar)) {
            final Resource object = (Resource) objVar.getValue();
            if (inferenceEngine.isEnumeratedType(object)) {
                final Set<BindingSet> solutions = new LinkedHashSet<>();
                final Set<Resource> enumeration = inferenceEngine.getEnumeration(object);
                for (final Resource enumType : enumeration) {
                    final QueryBindingSet qbs = new QueryBindingSet();
                    qbs.addBinding(subVar.getName(), enumType);
                    solutions.add(qbs);
                }

                if (!solutions.isEmpty()) {
                    final BindingSetAssignment enumNode = new BindingSetAssignment();
                    enumNode.setBindingSets(solutions);

                    node.replaceWith(enumNode);
                    log.trace("Replacing node with inferred one of enumeration: " + enumNode);
                }
            }
        }
    }
}