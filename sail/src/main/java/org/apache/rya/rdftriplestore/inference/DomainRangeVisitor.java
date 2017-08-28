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

import java.util.Set;
import java.util.UUID;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.utils.NullableStatementImpl;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;

/**
 * Expands the query tree to account for any relevant domain and range information known to the
 * {@link InferenceEngine}.
 *
 * Given a triple <:s :p :o>, if :p has an rdfs:domain of :D and rdfs:range of :R, the semantics of
 * domain and range imply that :s has type :D and that :o has type :R.
 *
 * Only operates on {@link StatementPattern} nodes whose form is <?subject rdfs:type :DefinedClass>.
 * If the class is the domain of any predicate, as reported by the {@link InferenceEngine}, a
 * subtree is constructed to infer the type based on the domain. If the class is the range of any
 * predicate, a subtree is similarly constructed to infer the type based on the range. If one or
 * both apply, then the original node is replaced with the union of the new subtree(s) and the
 * original statement pattern.
 *
 * If there are multiple ways to infer the type for one resource, the resulting query tree will
 * match all of them and produce multiple solutions for that resource.
 */
public class DomainRangeVisitor extends AbstractInferVisitor {
    /**
     * Creates a new {@link DomainRangeVisitor}.
     * @param conf The {@link RdfCloudTripleStoreConfiguration}.
     * @param inferenceEngine The InferenceEngine containing the relevant ontology.
     */
    public DomainRangeVisitor(RdfCloudTripleStoreConfiguration conf, InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = conf.isInferDomainRange();
    }

    /**
     * Checks whether this statement pattern might be inferred using domain and/or range knowledge,
     * and, if so, replaces the statement pattern with a union of itself and any possible
     * derivations.
     */
    @Override
    protected void meetSP(StatementPattern node) throws Exception {
        final Var subjVar = node.getSubjectVar();
        final Var predVar = node.getPredicateVar();
        final Var objVar = node.getObjectVar();
        final Var contextVar = node.getContextVar();
        // Only applies to statement patterns that query for members of a defined type.
        if (predVar != null && RDF.TYPE.equals(predVar.getValue())
                && objVar != null && objVar.getValue() instanceof URI) {
            final URI inferredType = (URI) objVar.getValue();
            // Preserve the original node so explicit type assertions are still matched:
            TupleExpr currentNode = node.clone();
            // If there are any properties with this type as domain, check for appropriate triples:
            final Set<URI> domainProperties = inferenceEngine.getPropertiesWithDomain(inferredType);
            if (!domainProperties.isEmpty()) {
                Var domainPredVar = new Var("p-" + UUID.randomUUID());
                Var domainObjVar = new Var("o-" + UUID.randomUUID());
                domainObjVar.setAnonymous(true);
                Var domainVar = new Var(RDFS.DOMAIN.stringValue(), RDFS.DOMAIN);
                StatementPattern domainSP = new DoNotExpandSP(subjVar, domainPredVar, domainObjVar, contextVar);
                // Enumerate predicates having this type as domain
                FixedStatementPattern domainFSP = new FixedStatementPattern(domainPredVar, domainVar, objVar);
                for (URI property : domainProperties) {
                    domainFSP.statements.add(new NullableStatementImpl(property, RDFS.DOMAIN, inferredType));
                }
                // For each such predicate, any triple <subjVar predicate _:any> implies the type
                currentNode = new InferUnion(currentNode, new InferJoin(domainFSP, domainSP));
            }
            // If there are any properties with this type as range, check for appropriate triples:
            final Set<URI> rangeProperties = inferenceEngine.getPropertiesWithRange(inferredType);
            if (!rangeProperties.isEmpty()) {
                Var rangePredVar = new Var("p-" + UUID.randomUUID());
                Var rangeSubjVar = new Var("s-" + UUID.randomUUID());
                rangeSubjVar.setAnonymous(true);
                Var rangeVar = new Var(RDFS.RANGE.stringValue(), RDFS.RANGE);
                StatementPattern rangeSP = new DoNotExpandSP(rangeSubjVar, rangePredVar, subjVar, contextVar);
                // Enumerate predicates having this type as range
                FixedStatementPattern rangeFSP = new FixedStatementPattern(rangePredVar, rangeVar, objVar);
                for (URI property : rangeProperties) {
                    rangeFSP.statements.add(new NullableStatementImpl(property, RDFS.RANGE, inferredType));
                }
                // For each such predicate, any triple <_:any predicate subjVar> implies the type
                currentNode = new InferUnion(currentNode, new InferJoin(rangeFSP, rangeSP));
            }
            node.replaceWith(currentNode);
        }
    }
}
