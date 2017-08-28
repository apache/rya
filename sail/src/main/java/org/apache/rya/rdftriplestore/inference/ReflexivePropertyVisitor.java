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

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.ZeroLengthPath;

/**
 * Expands the query tree to account for any relevant reflexive properties
 * known to the {@link InferenceEngine}.
 *
 * A reflexive property is a property for which any node can be inferred to have
 * reflexively: If :p is a reflexive property, then <?x :p ?x> is true for all ?x.
 *
 * Applies to any statement pattern whose predicate is defined (not a variable)
 * and is a reflexive property according to the InferenceEngine. If the property
 * is reflexive, then the statement pattern should match when the subject equals
 * the object. Therefore, replace the statement pattern with a union of itself
 * and a ZeroLengthPath between the subject and object. This union is similar to
 * the ZeroOrOnePath property path expression in SPARQL: <?x :p? ?y> matches if
 * ?x and ?y are connected via :p or if ?x and ?y are equal.
 */
public class ReflexivePropertyVisitor extends AbstractInferVisitor {
    /**
     * Creates a new {@link ReflexivePropertyVisitor}.
     * @param conf The {@link RdfCloudTripleStoreConfiguration}.
     * @param inferenceEngine The InferenceEngine containing the relevant ontology.
     */
    public ReflexivePropertyVisitor(RdfCloudTripleStoreConfiguration conf, InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = conf.isInferReflexiveProperty();
    }

    /**
     * Check whether any solution for the {@link StatementPattern} could be derived from
     * reflexive property inference, and if so, replace the pattern with a union of itself and the
     * reflexive solution.
     */
    @Override
    protected void meetSP(StatementPattern node) throws Exception {
        // Only applies when the predicate is defined and reflexive
        final Var predVar = node.getPredicateVar();
        if (predVar.getValue() != null && inferenceEngine.isReflexiveProperty((URI) predVar.getValue())) {
            final StatementPattern originalSP = node.clone();
            // The reflexive solution is a ZeroLengthPath between subject and
            // object: they can be matched to one another, whether constants or
            // variables.
            final Var subjVar = node.getSubjectVar();
            final Var objVar = node.getObjectVar();
            final ZeroLengthPath reflexiveSolution = new ZeroLengthPath(subjVar, objVar);
            node.replaceWith(new InferUnion(originalSP, reflexiveSolution));
        }
    }
}
