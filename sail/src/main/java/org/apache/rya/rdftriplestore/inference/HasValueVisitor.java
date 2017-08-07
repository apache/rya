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
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;

/**
 * Expands the query tree to account for any relevant has-value class
 * expressions in the ontology known to the {@link InferenceEngine}.
 *
 * Only operates on {@link StatementPattern} nodes, and only those including a
 * defined type or defined predicate which is relevant to a has-value
 * expression in the ontology. When applicable, replaces the node with one or
 * more nested {@link InferUnion}s, one of whose leaves is the original
 * StatementPattern.
 *
 * A has-value class expression references a specific predicate and a specific
 * value (object or literal), and represents the set of all individuals having
 * the specified value for the specified predicate. This has two implications
 * for inference: 1) If an individual has the specified value for the specified
 * predicate, then it implicitly belongs to the has-value class expression; and
 * 2) If an individual belongs to the has-value class expression, then it
 * implicitly has the specified value for the specified predicate.
 *
 * To handle the first case, the visitor expands statement patterns of the form
 * "?individual rdf:type :class" if the class or any of its subclasses are known
 * to be has-value class expressions. (Does not apply if the class term
 * is a variable.) The resulting query tree will match individuals who
 * implicitly belong to the class expression because they have the specified
 * value for the specified property, as well as any individuals explicitly
 * stated to belong to the class.
 *
 * To handle the second case, the visitor expands statement patterns of the form
 * "?individual :predicate ?value" if the predicate is known to be referenced by
 * any has-value expression. (Does not apply if the predicate term is a
 * variable.) The resulting query tree will match individuals and values that
 * can be derived from the individual's membership in a has-value class
 * expression (which itself may be explicit or derived from membership in a
 * subclass of the has-value class expression), as well as any individuals and
 * values explicitly stated.
 */
public class HasValueVisitor extends AbstractInferVisitor {
    /**
     * Creates a new {@link HasValueVisitor}, which is enabled by default.
     * @param conf The {@link RdfCloudTripleStoreConfiguration}.
     * @param inferenceEngine The InferenceEngine containing the relevant ontology.
     */
    public HasValueVisitor(RdfCloudTripleStoreConfiguration conf, InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = true;
    }

    /**
     * Checks whether facts matching the StatementPattern could be derived using
     * has-value inference, and if so, replaces the StatementPattern node with a
     * union of itself and any such possible derivations.
     */
    @Override
    protected void meetSP(StatementPattern node) throws Exception {
        final Var subjVar = node.getSubjectVar();
        final Var predVar = node.getPredicateVar();
        final Var objVar = node.getObjectVar();
        // We can reason over two types of statement patterns:
        // { ?var rdf:type :Restriction } and { ?var :property ?value }
        // Both require defined predicate
        if (predVar != null && predVar.getValue() != null) {
            final URI predURI = (URI) predVar.getValue();
            if (RDF.TYPE.equals(predURI) && objVar != null && objVar.getValue() != null
                    && objVar.getValue() instanceof Resource) {
                // If the predicate is rdf:type and the type is specified, check whether it can be
                // inferred using any hasValue restriction(s)
                final Resource objType = (Resource) objVar.getValue();
                final Map<URI, Set<Value>> sufficientValues = inferenceEngine.getHasValueByType(objType);
                if (sufficientValues.size() > 0) {
                    final Var valueVar = new Var("v-" + UUID.randomUUID());
                    TupleExpr currentNode = node.clone();
                    for (URI property : sufficientValues.keySet()) {
                        final Var propVar = new Var(property.toString(), property);
                        final TupleExpr valueSP = new DoNotExpandSP(subjVar, propVar, valueVar);
                        final FixedStatementPattern relevantValues = new FixedStatementPattern(objVar, propVar, valueVar);
                        for (Value value : sufficientValues.get(property)) {
                            relevantValues.statements.add(new NullableStatementImpl(objType, property, value));
                        }
                        currentNode = new InferUnion(currentNode, new InferJoin(relevantValues, valueSP));
                    }
                    node.replaceWith(currentNode);
                }
            }
            else {
                // If the predicate has some hasValue restriction associated with it, then finding
                // that the object belongs to the appropriate type implies a value.
                final Map<Resource, Set<Value>> impliedValues = inferenceEngine.getHasValueByProperty(predURI);
                if (impliedValues.size() > 0) {
                    final Var rdfTypeVar = new Var(RDF.TYPE.stringValue(), RDF.TYPE);
                    final Var typeVar = new Var("t-" + UUID.randomUUID());
                    final Var hasValueVar = new Var(OWL.HASVALUE.stringValue(), OWL.HASVALUE);
                    final TupleExpr typeSP = new DoNotExpandSP(subjVar, rdfTypeVar, typeVar);
                    final FixedStatementPattern typeToValue = new FixedStatementPattern(typeVar, hasValueVar, objVar);
                    final TupleExpr directValueSP = node.clone();
                    for (Resource type : impliedValues.keySet()) {
                        // { ?var rdf:type :type } implies { ?var :property :val } for certain (:type, :val) pairs
                        for (Value impliedValue : impliedValues.get(type)) {
                            typeToValue.statements.add(new NullableStatementImpl(type, OWL.HASVALUE, impliedValue));
                        }
                    }
                    node.replaceWith(new InferUnion(new InferJoin(typeToValue, typeSP), directValueSP));
                }
            }
        }
    }
}
