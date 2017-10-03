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
import java.util.List;
import java.util.UUID;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;

/**
 * All predicates are changed
 * Class SubPropertyOfVisitor
 * Date: Mar 29, 2011
 * Time: 11:28:34 AM
 */
public class PropertyChainVisitor extends AbstractInferVisitor {

    public PropertyChainVisitor(final RdfCloudTripleStoreConfiguration conf, final InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = conf.isInferPropertyChain();
    }


    @Override
    protected void meetSP(final StatementPattern node) throws Exception {
        final StatementPattern sp = node.clone();
        final Var predVar = sp.getPredicateVar();

        final URI pred = (URI) predVar.getValue();
        final String predNamespace = pred.getNamespace();

        final Var objVar = sp.getObjectVar();
        final Var cntxtVar = sp.getContextVar();
        if (objVar != null &&
                !RDF.NAMESPACE.equals(predNamespace) &&
                !SESAME.NAMESPACE.equals(predNamespace) &&
                !RDFS.NAMESPACE.equals(predNamespace)
                && !EXPANDED.equals(cntxtVar)) {

            final URI chainPropURI = (URI) predVar.getValue();
            final List<URI> chain = inferenceEngine.getPropertyChain(chainPropURI);
            final List<StatementPattern> expandedPatterns = new ArrayList<StatementPattern>();
            if (chain.size() > 0) {
                final Var originalSubj = sp.getSubjectVar();
                final Var originalObj = sp.getObjectVar();

                Var nextSubj = originalSubj;
                StatementPattern lastStatementPatternAdded = null;
                for (final URI chainElement : chain ){
                    final String s = UUID.randomUUID().toString();
                    final Var currentObj = new Var("c-" + s);
                    StatementPattern statementPattern = new StatementPattern(nextSubj, new Var(chainElement.stringValue()), currentObj, sp.getContextVar());
                    if (chainElement instanceof InverseURI){
                        statementPattern = new StatementPattern(currentObj, new Var(chainElement.stringValue()), nextSubj, sp.getContextVar());
                    }
                    expandedPatterns.add(statementPattern);
                    lastStatementPatternAdded = statementPattern;
                    nextSubj = currentObj;

                }
                if (lastStatementPatternAdded == null) {
                    throw new NullPointerException("lastStatementPatternAdded is null despite non-empty inferenceEngine property chain. chain.size()==" + chain.size());
                }
                lastStatementPatternAdded.setObjectVar(originalObj);

                TupleExpr lastRight = null;
                // replace the statement pattern with a series of joins
                for (final StatementPattern pattern : expandedPatterns){
                    if (lastRight == null){
                        lastRight = pattern;
                    }
                    else {
                        final Join newJoin = new Join(pattern, lastRight);
                        lastRight = newJoin;
                    }
                }
                if (lastRight != null){
                    node.replaceWith(lastRight);
                }

            }
        }
    }
}
