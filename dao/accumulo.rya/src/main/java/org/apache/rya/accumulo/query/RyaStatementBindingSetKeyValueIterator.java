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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.openrdf.query.BindingSet;

/**
 * Date: 7/17/12
 * Time: 11:48 AM
 */
public class RyaStatementBindingSetKeyValueIterator implements CloseableIteration<Map.Entry<RyaStatement, BindingSet>, RyaDAOException> {
    private Iterator<Map.Entry<Key, Value>> dataIterator;
    private TABLE_LAYOUT tableLayout;
    private Long maxResults = -1L;
    private ScannerBase scanner;
    private boolean isBatchScanner;
    private RangeBindingSetEntries rangeMap;
    private Iterator<BindingSet> bsIter;
    private RyaStatement statement;
	private RyaTripleContext ryaContext;

    public RyaStatementBindingSetKeyValueIterator(TABLE_LAYOUT tableLayout, RyaTripleContext context, ScannerBase scannerBase, RangeBindingSetEntries rangeMap) {
        this(tableLayout, ((scannerBase instanceof BatchScanner) ? ((BatchScanner) scannerBase).iterator() : ((Scanner) scannerBase).iterator()), rangeMap, context);
        this.scanner = scannerBase;
        isBatchScanner = scanner instanceof BatchScanner;
    }

    public RyaStatementBindingSetKeyValueIterator(TABLE_LAYOUT tableLayout, Iterator<Map.Entry<Key, Value>> dataIterator, RangeBindingSetEntries rangeMap, RyaTripleContext ryaContext) {
        this.tableLayout = tableLayout;
        this.rangeMap = rangeMap;
        this.dataIterator = dataIterator;
        this.ryaContext = ryaContext;
    }

    @Override
    public void close() throws RyaDAOException {
        dataIterator = null;
        if (scanner != null && isBatchScanner) {
            ((BatchScanner) scanner).close();
        }
    }

    public boolean isClosed() throws RyaDAOException {
        return dataIterator == null;
    }

    @Override
    public boolean hasNext() throws RyaDAOException {
        if (isClosed()) {
            return false;
        }
        if (maxResults != 0) {
            if (bsIter != null && bsIter.hasNext()) {
                return true;
            }
            if (dataIterator.hasNext()) {
                return true;
            } else {
                maxResults = 0l;
                return false;
            }
        }
        return false;
    }

    @Override
    public Map.Entry<RyaStatement, BindingSet> next() throws RyaDAOException {
        if (!hasNext() || isClosed()) {
            throw new NoSuchElementException();
        }

        try {
            while (true) {
                if (bsIter != null && bsIter.hasNext()) {
                    maxResults--;
                    return new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(statement, bsIter.next());
                }

                if (dataIterator.hasNext()) {
                    Map.Entry<Key, Value> next = dataIterator.next();
                    Key key = next.getKey();
                    statement = ryaContext.deserializeTriple(tableLayout,
                            new TripleRow(key.getRowData().toArray(), key.getColumnFamilyData().toArray(), key.getColumnQualifierData().toArray(),
                                    key.getTimestamp(), key.getColumnVisibilityData().toArray(), next.getValue().get()));
                    if (next.getValue() != null) {
                        statement.setValue(next.getValue().get());
                    }
                    Collection<BindingSet> bindingSets = rangeMap.containsKey(key);
                    if (!bindingSets.isEmpty()) {
                        bsIter = bindingSets.iterator();
                    }
                } else {
                    break;
                }
            }
            return null;
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
