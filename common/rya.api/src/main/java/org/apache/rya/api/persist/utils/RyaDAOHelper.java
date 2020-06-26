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

import com.google.common.base.Preconditions;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaResource;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.utils.WildcardStatement;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * TODO: Check there are sufficient unit tests in the DAOs to cover these methods...
 */
public class RyaDAOHelper {

    /**
     * This is a method used by {@link org.eclipse.rdf4j.query.algebra.evaluation.TripleSource}.
     * @param queryEngine
     * @param subject
     * @param predicate
     * @param object
     * @param conf
     * @param contexts
     * @return
     * @throws RyaDAOException
     */
    public static CloseableIteration<Statement, QueryEvaluationException> queryRdf4j(RyaQueryEngine queryEngine, Resource subject, IRI predicate, Value object, RdfCloudTripleStoreConfiguration conf, Resource... contexts) throws RyaDAOException {
        Preconditions.checkState(!(subject instanceof RyaResource), "Defensive coding. This method shouldn't be used with Rya types.");
        Preconditions.checkState(!(predicate instanceof RyaIRI), "Defensive coding. This method shouldn't be used with Rya types.");
        Preconditions.checkState(!(object instanceof RyaValue), "Defensive coding. This method shouldn't be used with Rya types.");
        Preconditions.checkState(!(contexts instanceof RyaResource[]), "Defensive coding. This method shouldn't be used with Rya types.");
        Collection<Map.Entry<Statement, BindingSet>> statements = new ArrayList<>(1);
        if (contexts != null && contexts.length > 0) {
            for (Resource context : contexts) {
                statements.add(new RdfCloudTripleStoreUtils.CustomEntry<>(
                        new WildcardStatement(subject, predicate, object, context), null));
            }
        } else {
            statements.add(new RdfCloudTripleStoreUtils.CustomEntry<>(
                    new WildcardStatement(subject, predicate, object), null));
        }
        CloseableIteration<Map.Entry<Statement, BindingSet>, QueryEvaluationException> query = queryRdf4j(queryEngine, statements, conf);
        // This translates from <Map.Entry<Statement, BindingSet>, QueryEvaluationException> to <Statement, QueryEvaluationException>
        return new CloseableIteration<Statement, QueryEvaluationException>() {

            private boolean isClosed = false;

            @Override
            public void close() throws RyaDAOException {
                try {
                    isClosed = true;
                    query.close();
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }

            @Override
            public boolean hasNext() throws RyaDAOException {
                try {
                    return query.hasNext();
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }

            @Override
            public Statement next() throws RyaDAOException {
                if (!hasNext() || isClosed) {
                    throw new NoSuchElementException();
                }

                try {
                    Map.Entry<Statement, BindingSet> next = query.next();
                    if (next == null || next.getKey() == null) {
                        return null;
                    }
                    return next.getKey();
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }

            @Override
            public void remove() throws RyaDAOException {
                try {
                    query.remove();
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }
        };
    }

    /**
     * This is a method used by {@link org.eclipse.rdf4j.query.algebra.evaluation.TripleSource}.
     * @param queryEngine
     * @param statements
     * @param conf
     * @return
     * @throws RyaDAOException
     */
    public static CloseableIteration<Map.Entry<Statement, BindingSet>, QueryEvaluationException> queryRdf4j(RyaQueryEngine queryEngine, Collection<Map.Entry<Statement, BindingSet>> statements, RdfCloudTripleStoreConfiguration conf) throws RyaDAOException {
        Preconditions.checkNotNull(conf, "Defensive coding. You should pass the configuration.");
        Collection<Map.Entry<RyaStatement, BindingSet>> ryaStatements = new ArrayList<>(statements.size());
        for (Map.Entry<Statement, BindingSet> entry : statements) {
            Statement statement = entry.getKey();
            BindingSet bindingSet = entry.getValue();
            ryaStatements.add(new RdfCloudTripleStoreUtils.CustomEntry<>(RdfToRyaConversions.convertStatement(statement), bindingSet));
        }
        CloseableIteration<Map.Entry<RyaStatement, BindingSet>, RyaDAOException> query = queryEngine.queryWithBindingSet(ryaStatements, conf);
        // This translates from <Map.Entry<RyaStatement, BindingSet>, RyaDAOException> to <Map.Entry<Statement, BindingSet>, QueryEvaluationException>
        return new CloseableIteration<Map.Entry<Statement, BindingSet>, QueryEvaluationException>() {

            private boolean isClosed = false;

            @Override
            public void close() throws RyaDAOException {
                try {
                    isClosed = true;
                    query.close();
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }

            @Override
            public boolean hasNext() throws RyaDAOException {
                try {
                    return query.hasNext();
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }

            @Override
            public Map.Entry<Statement, BindingSet> next() throws RyaDAOException {
                if (!hasNext() || isClosed) {
                    throw new NoSuchElementException();
                }

                try {
                    Map.Entry<RyaStatement, BindingSet> next = query.next();
                    if (next == null || next.getKey() == null) {
                        return null;
                    }
                    Statement statement = next.getKey().toStatement();
                    BindingSet bindingSet = next.getValue();
                    return new RdfCloudTripleStoreUtils.CustomEntry<>(statement, bindingSet);
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }

            @Override
            public void remove() throws RyaDAOException {
                try {
                    query.remove();
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }
        };
    }

    public static CloseableIteration<RyaStatement, RyaDAOException> query(RyaQueryEngine queryEngine, RyaResource subject, RyaIRI predicate, RyaValue object, RdfCloudTripleStoreConfiguration conf, RyaResource... contexts) throws RyaDAOException {
        List<RyaStatement> statements = new ArrayList<>();
        if (contexts != null && contexts.length > 0) {
            for (Resource context : contexts) {
                statements.add(new RyaStatement(subject, predicate, object, context));
            }
        } else {
            statements.add(new RyaStatement(subject, predicate, object));
        }
        return query(queryEngine, statements, conf);
    }

    public static CloseableIteration<RyaStatement, RyaDAOException> query(RyaQueryEngine queryEngine, RyaResource subject, RyaIRI predicate, RyaValue object, RdfCloudTripleStoreConfiguration conf) throws RyaDAOException {
        return query(queryEngine, new RyaStatement(subject, predicate, object), conf);
    }

    public static CloseableIteration<RyaStatement, RyaDAOException> query(RyaQueryEngine queryEngine, RyaStatement stmt, RdfCloudTripleStoreConfiguration conf) throws RyaDAOException {
        return query(queryEngine, Collections.singletonList(stmt), conf);
    }

    public static CloseableIteration<RyaStatement, RyaDAOException> query(RyaQueryEngine queryEngine, Iterable<RyaStatement> stmts, RdfCloudTripleStoreConfiguration conf) throws RyaDAOException {
        final Map<RyaStatement, BindingSet> statements = new HashMap<>();
        for (RyaStatement stmt : stmts) {
            statements.put(stmt, null);
        }
        CloseableIteration<Map.Entry<RyaStatement, BindingSet>, RyaDAOException> query = query(queryEngine, statements.entrySet(), conf);
        // This translates from <Map.Entry<RyaStatement, BindingSet>, RyaDAOException> to <RyaStatement, RyaDAOException>
        return new CloseableIteration<RyaStatement, RyaDAOException>() {

            private boolean isClosed = false;

            @Override
            public void close() throws RyaDAOException {
                try {
                    isClosed = true;
                    query.close();
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }

            @Override
            public boolean hasNext() throws RyaDAOException {
                try {
                    return query.hasNext();
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }

            @Override
            public RyaStatement next() throws RyaDAOException {
                if (!hasNext() || isClosed) {
                    throw new NoSuchElementException();
                }

                try {
                    Map.Entry<RyaStatement, BindingSet> next = query.next();
                    if (next == null || next.getKey() == null) {
                        return null;
                    }
                    return next.getKey();
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }

            @Override
            public void remove() throws RyaDAOException {
                try {
                    query.remove();
                } catch (RyaDAOException e) {
                    throw new RyaDAOException(e);
                }
            }
        };
    }

    public static CloseableIteration<Map.Entry<RyaStatement, BindingSet>, RyaDAOException> query(RyaQueryEngine queryEngine, Collection<Map.Entry<RyaStatement, BindingSet>> statements, RdfCloudTripleStoreConfiguration conf) throws RyaDAOException {
        Preconditions.checkNotNull(conf, "Defensive coding. You should pass the configuration.");
        Collection<Map.Entry<RyaStatement, BindingSet>> ryaStatements = new ArrayList<>(statements.size());
        for (Map.Entry<RyaStatement, BindingSet> entry : statements) {
            ryaStatements.add(new RdfCloudTripleStoreUtils.CustomEntry<>(entry.getKey(), entry.getValue()));
        }
        return queryEngine.queryWithBindingSet(ryaStatements, conf);
    }

}
