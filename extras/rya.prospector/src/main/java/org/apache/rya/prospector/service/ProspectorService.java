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
package org.apache.rya.prospector.service;

import static java.util.Objects.requireNonNull;
import static org.apache.rya.prospector.utils.ProspectorConstants.METADATA;
import static org.apache.rya.prospector.utils.ProspectorConstants.PROSPECT_TIME;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.rya.prospector.domain.IndexEntry;
import org.apache.rya.prospector.plans.IndexWorkPlan;
import org.apache.rya.prospector.plans.IndexWorkPlanManager;
import org.apache.rya.prospector.plans.impl.ServicesBackedIndexWorkPlanManager;
import org.apache.rya.prospector.utils.ProspectorUtils;

/**
 * Provides access to the Prospect results that have been stored within a specific Accumulo table.
 */
public class ProspectorService {

    private final Connector connector;
    private final String tableName;

    private final IndexWorkPlanManager manager = new ServicesBackedIndexWorkPlanManager();
    private final Map<String, IndexWorkPlan> plans;

    /**
     * Constructs an instance of {@link ProspectorService}.
     *
     * @param connector - The Accumulo connector used to communicate with the table. (not null)
     * @param tableName - The name of the Accumulo table that will be queried for Prospect results. (not null)
     * @throws AccumuloException A problem occurred while creating the table.
     * @throws AccumuloSecurityException A problem occurred while creating the table.
     */
    public ProspectorService(Connector connector, String tableName) throws AccumuloException, AccumuloSecurityException {
        this.connector = requireNonNull(connector);
        this.tableName = requireNonNull(tableName);

        this.plans = ProspectorUtils.planMap(manager.getPlans());

        // Create the table if it doesn't already exist.
        try {
            final TableOperations tos = connector.tableOperations();
            if(!tos.exists(tableName)) {
                tos.create(tableName);
            }
        } catch(TableExistsException e) {
            // Do nothing. Something else must have made it while we were.
        }
    }

    /**
     * Get a list of timestamps that represents all of the Prospect runs that have
     * ever been performed.
     *
     * @param auths - The authorizations used to scan the table for prospects.
     * @return A list of timestamps representing each Prospect run that was found.
     * @throws TableNotFoundException The table name that was provided when this
     *   class was constructed does not match a table that the connector has access to.
     */
    public Iterator<Long> getProspects(String[] auths) throws TableNotFoundException {
        final Scanner scanner = connector.createScanner(tableName, new Authorizations(auths));
        scanner.setRange(Range.exact(METADATA));
        scanner.fetchColumnFamily(new Text(PROSPECT_TIME));

        return new ProspectTimestampIterator( scanner.iterator() );
    }

    /**
     * Get a list of timestamps that represents all of the Prospect runs that
     * have been performed inclusively between two timestamps.
     *
     * @param beginTime - The start of the time range.
     * @param endTime - The end of the time range.
     * @param auths - The authorizations used to scan the table for prospects.
     * @return A list of timestamps representing each Prospect run that was found.
     * @throws TableNotFoundException The table name that was provided when this
     *   class was constructed does not match a table that the connector has access to.
     */
    public Iterator<Long> getProspectsInRange(long beginTime, long endTime, String[] auths) throws TableNotFoundException {
        final Scanner scanner = connector.createScanner(tableName, new Authorizations(auths));
        scanner.setRange(new Range(
                new Key(METADATA, PROSPECT_TIME, ProspectorUtils.getReverseIndexDateTime(new Date(endTime)), "", Long.MAX_VALUE),
                new Key(METADATA, PROSPECT_TIME, ProspectorUtils.getReverseIndexDateTime(new Date(beginTime)), "", 0l)
        ));

        return new ProspectTimestampIterator( scanner.iterator() );
    }

    /**
     * Iterates over the results of a {@link Scanner} and interprets their keys
     * contain Prospect run timestamps.
     */
    private static final class ProspectTimestampIterator implements Iterator<Long> {
        private final Iterator<Entry<Key, Value>> it;

        public ProspectTimestampIterator(Iterator<Entry<Key, Value>> it) {
            this.it = requireNonNull(it);
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Long next() {
            return it.next().getKey().getTimestamp();
        }
    }

    /**
     * Search for {@link IndexEntry}s that have values matching the provided parameters.
     *
     * @param prospectTimes - Indicates which Prospect runs will be part of the query.
     * @param indexType - The name of the index the {@link IndexEntry}s are stored within.
     * @param index - The data portion of the {@link IndexEntry}s that may be returned.
     * @param dataType - The data type of the {@link IndexEntry}s that may be returned.
     * @param auths - The authorizations used to search for the entries.
     * @return The {@link IndexEntries} that match the provided values.
     * @throws TableNotFoundException No table exists for {@code tableName}.
     */
    public List<IndexEntry> query(List<Long> prospectTimes, String indexType, String type, List<String> index, String dataType, String[] auths) throws TableNotFoundException {
        assert indexType != null;

        final IndexWorkPlan plan = plans.get(indexType);
        assert plan != null: "Index Type: ${indexType} does not exist";
        final String compositeIndex = plan.getCompositeValue(index);

        return plan.query(connector, tableName, prospectTimes, type, compositeIndex, dataType, auths);
    }
}