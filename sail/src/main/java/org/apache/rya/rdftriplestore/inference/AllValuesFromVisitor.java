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

import org.apache.rya.api.RdfTripleStoreConfiguration;
import org.apache.rya.api.utils.NullableStatementImpl;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

/**
 * Expands the query tree to account for any universal class expressions (property restrictions
 * using owl:allValuesFrom) in the ontology known to the {@link InferenceEngine}.
 *
 * Operates on {@link StatementPattern} nodes whose predicate is rdf:type and whose object is a
 * defined type (not a variable) which is related to an allValuesFrom expression in the ontology.
 * When applicable, replaces the node with a union of itself and a subtree that matches any
 * instance that can be inferred to have the type in question via the semantics of
 * owl:allValuesFrom.
 *
 * A universal class expression references a predicate and a value class, and represents the set of
 * individuals who, for any instance of the predicate, have a value belonging to the value class.
 * Therefore, the value class should be inferred for any individual which is the object of a triple
 * with that predicate and with a subject belonging to the class expression. This implication is
 * similar to rdfs:range except that it only applies when the subject of the triple belongs to a
 * specific type.
 *
 * (Note: Because of OWL's open world assumption, the inference in the other direction can't be
 * made. That is, when an individual is explicitly declared to have the universally quantified
 * restriction, then the types of its values can be inferred. But when the universal restriction
 * isn't explicitly stated, it can't be inferred from the values themselves, because there's no
 * guarantee that all values are known.)
 */
public class AllValuesFromVisitor extends AbstractInferVisitor {

    /**
     * Creates a new {@link AllValuesFromVisitor}.
     * @param conf The {@link RdfTripleStoreConfiguration}.
     * @param inferenceEngine The InferenceEngine containing the relevant ontology.
     */
    public AllValuesFromVisitor(RdfTripleStoreConfiguration conf, InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = conf.isInferAllValuesFrom();
    }

    /**
     * Checks whether the StatementPattern is a type query whose solutions could be inferred
     * by allValuesFrom inference, and if so, replaces the node with a union of itself and any
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
            Map<Resource, Set<IRI>> relevantAvfRestrictions = inferenceEngine.getAllValuesFromByValueType(typeToInfer);
            if (!relevantAvfRestrictions.isEmpty()) {
                // We can infer the queried type if, for an allValuesFrom restriction type
                // associated  with the queried type, some anonymous neighboring node belongs to the
                // restriction type and has the node in question (subjVar) as a value for the
                // restriction's property.
                final Var avfTypeVar = new Var("t-" + UUID.randomUUID());
                final Var avfPredVar = new Var("p-" + UUID.randomUUID());
                final Var neighborVar = new Var("n-" + UUID.randomUUID());
                neighborVar.setAnonymous(true);
                final StatementPattern membershipPattern = new DoNotExpandSP(neighborVar,
                        new Var(RDF.TYPE.stringValue(), RDF.TYPE), avfTypeVar);
                final StatementPattern valuePattern = new StatementPattern(neighborVar, avfPredVar, subjVar);
                final InferJoin avfPattern = new InferJoin(membershipPattern, valuePattern);
                // Use a FixedStatementPattern to contain the appropriate (restriction, predicate)
                // pairs, and check each one against the general pattern.
                final FixedStatementPattern avfPropertyTypes = new FixedStatementPattern(avfTypeVar,
                        new Var(OWL.ONPROPERTY.stringValue(), OWL.ONPROPERTY), avfPredVar);
                for (Resource avfRestrictionType : relevantAvfRestrictions.keySet()) {
                    for (IRI avfProperty : relevantAvfRestrictions.get(avfRestrictionType)) {
                        avfPropertyTypes.statements.add(new NullableStatementImpl(avfRestrictionType,
                                OWL.ONPROPERTY, avfProperty));
                    }
                }
                final InferJoin avfInferenceQuery = new InferJoin(avfPropertyTypes, avfPattern);
                node.replaceWith(new InferUnion(node.clone(), avfInferenceQuery));
            }
        }
    }
}
