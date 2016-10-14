package org.apache.rya.accumulo.query;

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

import java.util.Iterator;
import java.util.Map;

import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Date: 7/17/12
 * Time: 11:48 AM
 */
public class RyaStatementKeyValueIterator implements CloseableIteration<RyaStatement, RyaDAOException> {
    private Iterator<Map.Entry<Key, Value>> dataIterator;
    private TABLE_LAYOUT tableLayout;
    private Long maxResults = -1L;
    private RyaTripleContext context;

    public RyaStatementKeyValueIterator(TABLE_LAYOUT tableLayout, RyaTripleContext context, Iterator<Map.Entry<Key, Value>> dataIterator) {
        this.tableLayout = tableLayout;
        this.dataIterator = dataIterator;
        this.context = context;
    }

    @Override
    public void close() throws RyaDAOException {
        dataIterator = null;
    }

    public boolean isClosed() throws RyaDAOException {
        return dataIterator == null;
    }

    @Override
    public boolean hasNext() throws RyaDAOException {
        if (isClosed()) {
            throw new RyaDAOException("Closed Iterator");
        }
        return maxResults != 0 && dataIterator.hasNext();
    }

    @Override
    public RyaStatement next() throws RyaDAOException {
        if (!hasNext()) {
            return null;
        }

        try {
            Map.Entry<Key, Value> next = dataIterator.next();
            Key key = next.getKey();
            RyaStatement statement = context.deserializeTriple(tableLayout,
                    new TripleRow(key.getRowData().toArray(), key.getColumnFamilyData().toArray(), key.getColumnQualifierData().toArray(),
                            key.getTimestamp(), key.getColumnVisibilityData().toArray(), next.getValue().get()));
            if (next.getValue() != null) {
                statement.setValue(next.getValue().get());
            }
            maxResults--;
            return statement;
        } catch (TripleRowResolverException e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public void remove() throws RyaDAOException {
        next();
    }

    public Long getMaxResults() {
        return maxResults;
    }

    public void setMaxResults(Long maxResults) {
        this.maxResults = maxResults;
    }
}
