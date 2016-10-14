package org.apache.rya.indexing;

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


import info.aduna.iteration.CloseableIteration;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.MapBindingSet;


//Given StatementPattern constraint and SearchFunction associated with an Indexing Node,
//creates appropriate StatementConstraints object from StatementPattern constraint and
//binding set and then uses SearchFunction to delegate query to appropriate index.
//Resulting iterator over statements is then converted to an iterator over binding sets
public class IteratorFactory {

    public static CloseableIteration<BindingSet, QueryEvaluationException> getIterator(final StatementPattern match, 
            final BindingSet bindings, final String queryText, final SearchFunction searchFunction) {
        return new CloseableIteration<BindingSet, QueryEvaluationException>() {

            private boolean isClosed = false;
            private CloseableIteration<Statement, QueryEvaluationException> statementIt = null;

            private String subjectBinding = match.getSubjectVar().getName();
            private String predicateBinding = match.getPredicateVar().getName();
            private String objectBinding = match.getObjectVar().getName();
            private String contextBinding = null;

            private void performQuery() throws QueryEvaluationException {

                StatementConstraints contraints = new StatementConstraints();

                // get the context (i.e. named graph) of the statement and use that in the query
                QueryModelNode parentNode = match.getSubjectVar().getParentNode();
                if (parentNode instanceof StatementPattern) {
                    StatementPattern parentStatement = (StatementPattern) parentNode;
                    Var contextVar = parentStatement.getContextVar();
                    if (contextVar != null) {
                        contextBinding = contextVar.getName();
                        Resource context = (Resource) contextVar.getValue();
                        contraints.setContext(context);
                    }
                }

                // get the subject constraint
                if (match.getSubjectVar().isConstant()) {
                    // get the subject binding from the filter/statement pair
                    Resource subject = (Resource) match.getSubjectVar().getValue();
                    contraints.setSubject(subject);
                } else if (bindings.hasBinding(subjectBinding)) {
                    // get the subject binding from the passed in bindings (eg from other statements/parts of the tree)
                    Resource subject = (Resource) bindings.getValue(subjectBinding);
                    contraints.setSubject(subject);
                }

                // get the predicate constraint
                if (match.getPredicateVar().isConstant()) {
                    // get the predicate binding from the filter/statement pair
                    Set<URI> predicates = new HashSet<URI>(getPredicateRestrictions(match.getPredicateVar()));
                    contraints.setPredicates(predicates);
                } else if (bindings.hasBinding(predicateBinding)) {
                    // get the predicate binding from the passed in bindings (eg from other statements/parts of the tree)
                    URI predicateUri = (URI) bindings.getValue(predicateBinding);
                    Set<URI> predicates = Collections.singleton(predicateUri);
                    contraints.setPredicates(predicates);
                }

                statementIt = searchFunction.performSearch(queryText, contraints);
            }

            @Override
            public boolean hasNext() throws QueryEvaluationException {
                if (statementIt == null) {
                    performQuery();
                }
                return statementIt.hasNext();
            }

            @Override
            public BindingSet next() throws QueryEvaluationException {
                if (!hasNext() || isClosed) {
                    throw new NoSuchElementException();
                }

                Statement statment = statementIt.next();

                MapBindingSet bset = new MapBindingSet();
                if (!subjectBinding.startsWith("-const"))
                    bset.addBinding(subjectBinding, statment.getSubject());
                if (!predicateBinding.startsWith("-const"))
                    bset.addBinding(predicateBinding, statment.getPredicate());
                if (!objectBinding.startsWith("-const"))
                    bset.addBinding(objectBinding, statment.getObject());
                if (contextBinding != null && !contextBinding.startsWith("-const"))
                    bset.addBinding(contextBinding, statment.getContext());

                // merge with other bindings.
                for (String name : bindings.getBindingNames()) {
                    bset.addBinding(name, bindings.getValue(name));
                }

                return bset;
            }

            @Override
            public void remove() throws QueryEvaluationException {
                throw new UnsupportedOperationException();

            }

            @Override
            public void close() throws QueryEvaluationException {
                if (statementIt != null) {
                    statementIt.close();
                }
                isClosed = true;
            }

        };

    }
   
    public static Collection<URI> getPredicateRestrictions(Var predicate) {
        if (predicate.hasValue())
            return Collections.singleton((URI) predicate.getValue());
        return Collections.emptyList();
    }
}
