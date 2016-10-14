package org.apache.rya.api.persist.utils;

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
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.api.utils.NullableStatementImpl;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Date: 7/20/12
 * Time: 10:36 AM
 */
public class RyaDAOHelper {

    public static CloseableIteration<Statement, QueryEvaluationException> query(RyaDAO ryaDAO, Resource subject, URI predicate, Value object, RdfCloudTripleStoreConfiguration conf, Resource... contexts) throws QueryEvaluationException {
        return query(ryaDAO, new NullableStatementImpl(subject, predicate, object, contexts), conf);
    }

    public static CloseableIteration<Statement, QueryEvaluationException> query(RyaDAO ryaDAO, Statement stmt, RdfCloudTripleStoreConfiguration conf) throws QueryEvaluationException {
        final CloseableIteration<RyaStatement, RyaDAOException> query;
        try {
            query = ryaDAO.getQueryEngine().query(RdfToRyaConversions.convertStatement(stmt),
                    conf);
        } catch (RyaDAOException e) {
            throw new QueryEvaluationException(e);
        }
        //TODO: only support one context for now
        return new CloseableIteration<Statement, QueryEvaluationException>() {   //TODO: Create a new class struct for this

            private boolean isClosed = false;
            @Override
            public void close() throws QueryEvaluationException {
                try {
                    isClosed = true;
                    query.close();
                } catch (RyaDAOException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public boolean hasNext() throws QueryEvaluationException {
                try {
                    return query.hasNext();
                } catch (RyaDAOException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public Statement next() throws QueryEvaluationException {
                if (!hasNext() || isClosed) {
                    throw new NoSuchElementException();
                }

                try {
                    RyaStatement next = query.next();
                    if (next == null) {
                        return null;
                    }
                    return RyaToRdfConversions.convertStatement(next);
                } catch (RyaDAOException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public void remove() throws QueryEvaluationException {
                try {
                    query.remove();
                } catch (RyaDAOException e) {
                    throw new QueryEvaluationException(e);
                }
            }
        };
    }

    public static CloseableIteration<? extends Map.Entry<Statement, BindingSet>, QueryEvaluationException> query(RyaDAO ryaDAO, Collection<Map.Entry<Statement, BindingSet>> statements, RdfCloudTripleStoreConfiguration conf) throws QueryEvaluationException {
        Collection<Map.Entry<RyaStatement, BindingSet>> ryaStatements = new ArrayList<Map.Entry<RyaStatement, BindingSet>>(statements.size());
        for (Map.Entry<Statement, BindingSet> entry : statements) {
            ryaStatements.add(new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>
                    (RdfToRyaConversions.convertStatement(entry.getKey()), entry.getValue()));
        }
        final CloseableIteration<? extends Map.Entry<RyaStatement, BindingSet>, RyaDAOException> query;
        try {
            query = ryaDAO.getQueryEngine().queryWithBindingSet(ryaStatements, conf);
        } catch (RyaDAOException e) {
            throw new QueryEvaluationException(e);
        }
        return new CloseableIteration<Map.Entry<Statement, BindingSet>, QueryEvaluationException>() {   //TODO: Create a new class struct for this
            private boolean isClosed = false;

            @Override
            public void close() throws QueryEvaluationException {
                isClosed = true;
                try {
                    query.close();
                } catch (RyaDAOException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public boolean hasNext() throws QueryEvaluationException {
                try {
                    return query.hasNext();
                } catch (RyaDAOException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public Map.Entry<Statement, BindingSet> next() throws QueryEvaluationException {
                if (!hasNext() || isClosed) {
                    throw new NoSuchElementException();
                }
                try {

                    Map.Entry<RyaStatement, BindingSet> next = query.next();
                    if (next == null) {
                        return null;
                    }
                    return new RdfCloudTripleStoreUtils.CustomEntry<Statement, BindingSet>(RyaToRdfConversions.convertStatement(next.getKey()), next.getValue());
                } catch (RyaDAOException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public void remove() throws QueryEvaluationException {
                try {
                    query.remove();
                } catch (RyaDAOException e) {
                    throw new QueryEvaluationException(e);
                }
            }
        };
    }

}
