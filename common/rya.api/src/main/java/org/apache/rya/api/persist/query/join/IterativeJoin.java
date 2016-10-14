package org.apache.rya.api.persist.query.join;

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
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.*;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.api.resolver.RyaContext;
import org.openrdf.query.BindingSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Date: 7/24/12
 * Time: 8:52 AM
 */
public class IterativeJoin<C extends RdfCloudTripleStoreConfiguration> implements Join<C> {

    private RyaContext ryaContext = RyaContext.getInstance();
    private RyaQueryEngine ryaQueryEngine;

    public IterativeJoin() {
    }

    public IterativeJoin(RyaQueryEngine ryaQueryEngine) {
        this.ryaQueryEngine = ryaQueryEngine;
    }

    /**
     * Return all statements that have input predicates. Predicates must not be null or ranges
     *
     * @param preds
     * @return
     */
    @Override
    public CloseableIteration<RyaStatement, RyaDAOException> join(C conf, RyaURI... preds)
            throws RyaDAOException {
        Preconditions.checkNotNull(preds);
        Preconditions.checkArgument(preds.length > 1, "Must join 2 or more");
        //TODO: Reorder predObjs based on statistics

        CloseableIteration<RyaStatement, RyaDAOException> iter = null;
        for (RyaURI pred : preds) {
            if (iter == null) {
                iter = ryaQueryEngine.query(new RyaStatement(null, pred, null), null);
            } else {
                iter = join(iter, pred);
            }
        }

        return iter;
    }

    /**
     * Return all subjects that have the predicate objects associated. Predicate and objects must be not null or ranges
     * to ensure sorting
     *
     * @param predObjs
     * @return
     * @throws org.apache.rya.api.persist.RyaDAOException
     *
     */
    @Override
    public CloseableIteration<RyaURI, RyaDAOException> join(C conf, Map.Entry<RyaURI, RyaType>... predObjs)
            throws RyaDAOException {
        Preconditions.checkNotNull(predObjs);
        Preconditions.checkArgument(predObjs.length > 1, "Must join 2 or more");

        //TODO: Reorder predObjs based on statistics
        CloseableIteration<RyaStatement, RyaDAOException> first = null;
        CloseableIteration<RyaURI, RyaDAOException> iter = null;
        for (Map.Entry<RyaURI, RyaType> entry : predObjs) {
            if (first == null) {
                first = ryaQueryEngine.query(new RyaStatement(null, entry.getKey(), entry.getValue()), null);
            } else if (iter == null) {
                iter = join(new ConvertingIteration<RyaStatement, RyaURI, RyaDAOException>(first) {

                    @Override
                    protected RyaURI convert(RyaStatement statement) throws RyaDAOException {
                        return statement.getSubject();
                    }
                }, entry);
            } else {
                iter = join(iter, entry);
            }
        }

        return iter;
    }

    protected CloseableIteration<RyaURI, RyaDAOException> join(final CloseableIteration<RyaURI, RyaDAOException> iteration,
                                                               final Map.Entry<RyaURI, RyaType> predObj) {
        //TODO: configure batch
        //TODO: batch = 1, does not work
        final int batch = 100;
        return new CloseableIteration<RyaURI, RyaDAOException>() {

            private CloseableIteration<Map.Entry<RyaStatement, BindingSet>, RyaDAOException> query;

            @Override
            public void close() throws RyaDAOException {
                iteration.close();
                if (query != null) {
                    query.close();
                }
            }

            @Override
            public boolean hasNext() throws RyaDAOException {
                return !(query == null || !query.hasNext()) || batchNext();
            }

            @Override
            public RyaURI next() throws RyaDAOException {
                if (query == null || !query.hasNext()) {
                    if (!batchNext()) return null;
                }
                if (query != null && query.hasNext()) {
                    return query.next().getKey().getSubject();
                } else {
                    return null;
                }
            }

            private boolean batchNext() throws RyaDAOException {
                if (!iteration.hasNext()) {
                    return false;
                }
                Collection<Map.Entry<RyaStatement, BindingSet>> batchedResults = new ArrayList<Map.Entry<RyaStatement, BindingSet>>();
                for (int i = 0; i < batch && iteration.hasNext(); i++) {
                    batchedResults.add(new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(
                            new RyaStatement(iteration.next(), predObj.getKey(), predObj.getValue()), null));
                }
                query = ryaQueryEngine.queryWithBindingSet(batchedResults, null);
                return query.hasNext();
            }

            @Override
            public void remove() throws RyaDAOException {
                this.next();
            }
        };
    }

    protected CloseableIteration<RyaStatement, RyaDAOException> join(
            final CloseableIteration<RyaStatement, RyaDAOException> iteration, final RyaURI pred) {
        //TODO: configure batch
        //TODO: batch = 1, does not work
        final int batch = 100;
        return new CloseableIteration<RyaStatement, RyaDAOException>() {

            private CloseableIteration<Map.Entry<RyaStatement, BindingSet>, RyaDAOException> query;

            @Override
            public void close() throws RyaDAOException {
                iteration.close();
                if (query != null) {
                    query.close();
                }
            }

            @Override
            public boolean hasNext() throws RyaDAOException {
                return !(query == null || !query.hasNext()) || batchNext();
            }

            @Override
            public RyaStatement next() throws RyaDAOException {
                if (query == null || !query.hasNext()) {
                    if (!batchNext()) return null;
                }
                if (query != null && query.hasNext()) {
                    return query.next().getKey();
                } else {
                    return null;
                }
            }

            private boolean batchNext() throws RyaDAOException {
                if (!iteration.hasNext()) {
                    return false;
                }
                Collection<Map.Entry<RyaStatement, BindingSet>> batchedResults = new ArrayList<Map.Entry<RyaStatement, BindingSet>>();
                for (int i = 0; i < batch && iteration.hasNext(); i++) {
                    RyaStatement next = iteration.next();
                    batchedResults.add(new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(
                            new RyaStatement(next.getSubject(), pred, next.getObject()), null));
                }
                query = ryaQueryEngine.queryWithBindingSet(batchedResults, null);
                return query.hasNext();
            }

            @Override
            public void remove() throws RyaDAOException {
                this.next();
            }
        };
    }

    public RyaQueryEngine getRyaQueryEngine() {
        return ryaQueryEngine;
    }

    public void setRyaQueryEngine(RyaQueryEngine ryaQueryEngine) {
        this.ryaQueryEngine = ryaQueryEngine;
    }
}
