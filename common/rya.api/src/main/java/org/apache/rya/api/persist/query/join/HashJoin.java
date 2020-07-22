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

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaResource;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.api.persist.utils.RyaDAOHelper;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.utils.EnumerationWrapper;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Use HashTable to do a HashJoin.
 * <p/>
 * TODO: Somehow make a more streaming way of doing this hash join. This will not support large sets.
 * Date: 7/26/12
 * Time: 8:58 AM
 */
public class HashJoin<C extends RdfCloudTripleStoreConfiguration> implements Join<C> {

    private RyaContext ryaContext = RyaContext.getInstance();
    private RyaQueryEngine<C> ryaQueryEngine;

    public HashJoin() {
    }

    public HashJoin(RyaQueryEngine<C> ryaQueryEngine) {
        this.ryaQueryEngine = ryaQueryEngine;
    }

    @Override
    public CloseableIteration<RyaStatement, RyaDAOException> join(C conf, RyaIRI... preds) throws RyaDAOException {
        ConcurrentHashMap<Map.Entry<RyaResource, RyaValue>, Integer> ht = new ConcurrentHashMap<>();
        int count = 0;
        boolean first = true;
        for (RyaIRI pred : preds) {
            count++;
            //query
            CloseableIteration<RyaStatement, RyaDAOException> results = RyaDAOHelper.query(ryaQueryEngine, new RyaStatement(null, pred, null), conf);
            //add to hashtable
            while (results.hasNext()) {
                RyaStatement next = results.next();
                RyaResource subject = next.getSubject();
                RyaValue object = next.getObject();
                Map.Entry<RyaResource, RyaValue> entry = new RdfCloudTripleStoreUtils.CustomEntry<>(subject, object);
                if (!first) {
                    if (!ht.containsKey(entry)) {
                        continue; //not in join
                    }
                }
                ht.put(entry, count);
            }
            //remove from hashtable values that are under count
            if (first) {
                first = false;
            } else {
                for (Map.Entry<Map.Entry<RyaResource, RyaValue>, Integer> entry : ht.entrySet()) {
                    if (entry.getValue() < count) {
                        ht.remove(entry.getKey());
                    }
                }
            }
        }
        final Enumeration<Map.Entry<RyaResource, RyaValue>> keys = ht.keys();
        return new CloseableIteration<RyaStatement, RyaDAOException>() {
            @Override
            public void close() throws RyaDAOException {

            }

            @Override
            public boolean hasNext() throws RyaDAOException {
                return keys.hasMoreElements();
            }

            @Override
            public RyaStatement next() throws RyaDAOException {
                Map.Entry<RyaResource, RyaValue> subjObj = keys.nextElement();
                return new RyaStatement(subjObj.getKey(), null, subjObj.getValue());
            }

            @Override
            public void remove() throws RyaDAOException {
                keys.nextElement();
            }
        };
    }

    @Override
    public CloseableIteration<RyaResource, RyaDAOException> join(C conf, Map.Entry<RyaIRI, RyaValue>... predObjs) throws QueryEvaluationException {
        ConcurrentHashMap<RyaResource, Integer> ht = new ConcurrentHashMap<>();
        int count = 0;
        boolean first = true;
        for (Map.Entry<RyaIRI, RyaValue> predObj : predObjs) {
            count++;
            RyaIRI pred = predObj.getKey();
            RyaValue obj = predObj.getValue();
            //query
            CloseableIteration<RyaStatement, RyaDAOException> results = RyaDAOHelper.query(ryaQueryEngine, new RyaStatement(null, pred, obj), conf);
            //add to hashtable
            while (results.hasNext()) {
                RyaResource subject = results.next().getSubject();
                if (!first) {
                    if (!ht.containsKey(subject)) {
                        continue; //not in join
                    }
                }
                ht.put(subject, count);
            }
            //remove from hashtable values that are under count
            if (first) {
                first = false;
            } else {
                for (Map.Entry<RyaResource, Integer> entry : ht.entrySet()) {
                    if (entry.getValue() < count) {
                        ht.remove(entry.getKey());
                    }
                }
            }
        }
        return new EnumerationWrapper<>(ht.keys());
    }

    public RyaQueryEngine getRyaQueryEngine() {
        return ryaQueryEngine;
    }

    public void setRyaQueryEngine(RyaQueryEngine ryaQueryEngine) {
        this.ryaQueryEngine = ryaQueryEngine;
    }
}
