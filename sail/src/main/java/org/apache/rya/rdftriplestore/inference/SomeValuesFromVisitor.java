package org.apache.rya.rdftriplestore.inference;
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

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.utils.NullableStatementImpl;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

/**
 * Expands the query tree to account for any existential class expressions (property restrictions
 * using owl:someValuesFrom) in the ontology known to the {@link InferenceEngine}.
 *
 * Operates on {@link StatementPattern} nodes whose predicate is rdf:type and whose object is a
 * defined type (not a variable) which corresponds to a someValuesFrom expression in the ontology.
 * When applicable, replaces the node with a union of itself and a subtree that matches any instance
 * that can be inferred to have the type in question via the semantics of owl:someValuesFrom.
 *
 * An existential class expression references a predicate and a value class, and represents the set
 * of individuals with at least one value of that class for that predicate. Therefore, membership
 * in the class expression should be inferred for any individual which is the subject of a triple
 * with that predicate and with an object belonging to the value type. This implication is similar
 * to rdfs:domain except that it only applies when the object of the triple belongs to a specific
 * type.
 *
 * (Note: The inference in the other direction would be that, if an individual is declared to belong
 * to the class expression, then there exists some other individual which satisfies the requirement
 * that there is at least one value of the appropriate type. However, this other individual may be
 * any arbitrary resource, explicitly represented in the data or otherwise, so this implication is
 * not used.)
 */
public class SomeValuesFromVisitor extends AbstractInferVisitor {
    /**
     * Creates a new {@link SomeValuesFromVisitor}.
     * @param conf The {@link RdfCloudTripleStoreConfiguration}.
     * @param inferenceEngine The InferenceEngine containing the relevant ontology.
     */
    public SomeValuesFromVisitor(RdfCloudTripleStoreConfiguration conf, InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = conf.isInferSomeValuesFrom();
    }

    /**
     * Checks whether the StatementPattern is a type query whose solutions could be inferred by
     * someValuesFrom inference, and if so, replaces the node with a union of itself and any
     * possible inference.
     */
    @Override
    protected void meetSP(StatementPattern node) throws Exception {
        final Var subjVar = node.getSubjectVar();
        final Var predVar = node.getPredicateVar();
        final Var objVar = node.getObjectVar();
        // Only applies to type queries where the type is defined
        if (predVar != null && RDF.TYPE.equals(predVar.getValue()) && objVar != null && objVar.getValue() instanceof Resource) {
            final Resource typeToInfer = (Resource) objVar.getValue();
            Map<Resource, Set<IRI>> relevantSvfRestrictions = inferenceEngine.getSomeValuesFromByRestrictionType(typeToInfer);
            if (!relevantSvfRestrictions.isEmpty()) {
                // We can infer the queried type if it is to a someValuesFrom restriction (or a
                // supertype of one), and the node in question (subjVar) is the subject of a triple
                // whose predicate is the restriction's property and whose object is an arbitrary
                // node of the restriction's value type.
                final Var valueTypeVar = new Var("t-" + UUID.randomUUID());
                final Var svfPredVar = new Var("p-" + UUID.randomUUID());
                final Var neighborVar = new Var("n-" + UUID.randomUUID());
                neighborVar.setAnonymous(true);
                final StatementPattern membershipPattern = new DoNotExpandSP(neighborVar,
                        new Var(RDF.TYPE.stringValue(), RDF.TYPE), valueTypeVar);
                final StatementPattern valuePattern = new StatementPattern(subjVar, svfPredVar, neighborVar);
                final InferJoin svfPattern = new InferJoin(membershipPattern, valuePattern);
                // Use a FixedStatementPattern to contain the appropriate (predicate, value type)
                // pairs, and check each one against the general pattern.
                final FixedStatementPattern svfPropertyTypes = new FixedStatementPattern(svfPredVar,
                        new Var(OWL.SOMEVALUESFROM.stringValue(), OWL.SOMEVALUESFROM), valueTypeVar);
                for (Resource svfValueType : relevantSvfRestrictions.keySet()) {
                    for (IRI svfProperty : relevantSvfRestrictions.get(svfValueType)) {
                        svfPropertyTypes.statements.add(new NullableStatementImpl(svfProperty,
                                OWL.SOMEVALUESFROM, svfValueType));
                    }
                }
                final InferJoin svfInferenceQuery = new InferJoin(svfPropertyTypes, svfPattern);
                node.replaceWith(new InferUnion(node.clone(), svfInferenceQuery));
            }
        }
    }
}
