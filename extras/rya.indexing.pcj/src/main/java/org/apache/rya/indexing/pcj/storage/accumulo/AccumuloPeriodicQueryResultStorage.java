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
package org.apache.rya.indexing.pcj.storage.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.rya.indexing.pcj.storage.PCJIdFactory;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.AggregateOperatorBase;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.base.Preconditions;

/**
 * This class is the Accumulo implementation of {@link PeriodicQueryResultStorage} for
 * creating, deleting, and interacting with tables where PeriodicQuery results are stored.
 */
public class AccumuloPeriodicQueryResultStorage implements PeriodicQueryResultStorage {

    private final String ryaInstance;
    private final Connector accumuloConn;
    private Authorizations auths;
    private final PCJIdFactory pcjIdFactory = new PCJIdFactory();
    private final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();
    private static final PcjTables pcjTables = new PcjTables();
    private static final PeriodicQueryTableNameFactory tableNameFactory = new PeriodicQueryTableNameFactory();

    /**
     * Creates a AccumuloPeriodicQueryResultStorage Object.
     * @param accumuloConn - Accumulo Connector for connecting to an Accumulo instance
     * @param ryaInstance - Rya Instance name for connecting to Rya
     */
    public AccumuloPeriodicQueryResultStorage(final Connector accumuloConn, final String ryaInstance) {
        this.accumuloConn = Preconditions.checkNotNull(accumuloConn);
        this.ryaInstance = Preconditions.checkNotNull(ryaInstance);
        final String user = accumuloConn.whoami();
        try {
            this.auths = accumuloConn.securityOperations().getUserAuthorizations(user);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException("Unable access user: " + user + "authorizations.");
        }
    }

    @Override
    public String createPeriodicQuery(final String sparql) throws PeriodicQueryStorageException {
        Preconditions.checkNotNull(sparql);
        final String queryId = pcjIdFactory.nextId();
        return createPeriodicQuery(queryId, sparql);
    }

    @Override
    public String createPeriodicQuery(final String queryId, final String sparql) throws PeriodicQueryStorageException {
        Set<String> bindingNames;
        try {
            bindingNames = new AggregateVariableRemover().getNonAggregationVariables(sparql);
        } catch (final MalformedQueryException e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
        final List<String> varOrderList = new ArrayList<>();
        varOrderList.add(PeriodicQueryResultStorage.PeriodicBinId);
        varOrderList.addAll(bindingNames);
        createPeriodicQuery(queryId, sparql, new VariableOrder(varOrderList));
        return queryId;
    }

    @Override
    public void createPeriodicQuery(final String queryId, final String sparql, final VariableOrder order) throws PeriodicQueryStorageException {
        Preconditions.checkNotNull(sparql);
        Preconditions.checkNotNull(queryId);
        Preconditions.checkNotNull(order);
        Preconditions.checkArgument(PeriodicQueryResultStorage.PeriodicBinId.equals(order.getVariableOrders().get(0)),
                "periodicBinId binding name must occur first in VariableOrder.");
        final String tableName = tableNameFactory.makeTableName(ryaInstance, queryId);
        final Set<VariableOrder> varOrders = new HashSet<>();
        varOrders.add(order);
        try {
            pcjTables.createPcjTable(accumuloConn, tableName, varOrders, sparql);
        } catch (final Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
    }

    @Override
    public PeriodicQueryStorageMetadata getPeriodicQueryMetadata(final String queryId) throws PeriodicQueryStorageException {
        try {
            return new PeriodicQueryStorageMetadata(
                    pcjTables.getPcjMetadata(accumuloConn, tableNameFactory.makeTableName(ryaInstance, queryId)));
        } catch (final Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
    }

    @Override
    public void addPeriodicQueryResults(final String queryId, final Collection<VisibilityBindingSet> results) throws PeriodicQueryStorageException {
        results.forEach(x -> Preconditions.checkArgument(x.hasBinding(PeriodicQueryResultStorage.PeriodicBinId),
                "BindingSet must contain periodBinId binding."));
        try {
            pcjTables.addResults(accumuloConn, tableNameFactory.makeTableName(ryaInstance, queryId), results);
        } catch (final Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
    }

    @Override
    public void deletePeriodicQueryResults(final String queryId, final long binId) throws PeriodicQueryStorageException {
        final String tableName = tableNameFactory.makeTableName(ryaInstance, queryId);
        BatchDeleter deleter = null;
        try {
            final Text prefix = getRowPrefix(binId);
            deleter = accumuloConn.createBatchDeleter(tableName, auths, 1, new BatchWriterConfig());
            deleter.setRanges(Collections.singleton(Range.prefix(prefix)));
            deleter.delete();
        } catch (final Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        } finally {
            try {
                if(deleter != null) {
                    deleter.close();
                }
            } catch (final Exception e) {
                throw new PeriodicQueryStorageException(e.getMessage());
            }
        }
    }

    public void deletePeriodicQueryResults(final String queryId) throws PeriodicQueryStorageException {
        try {
            pcjTables.purgePcjTable(accumuloConn, tableNameFactory.makeTableName(ryaInstance, queryId));
        } catch (final Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
    }

    @Override
    public void deletePeriodicQuery(final String queryId) throws PeriodicQueryStorageException {
        try {
            pcjTables.dropPcjTable(accumuloConn, tableNameFactory.makeTableName(ryaInstance, queryId));
        } catch (final Exception e) {
            throw new PeriodicQueryStorageException(e.getMessage());
        }
    }

    @Override
    public CloseableIterator<BindingSet> listResults(final String queryId, final Optional<Long> binId)
            throws PeriodicQueryStorageException {
        requireNonNull(queryId);

        final String tableName = tableNameFactory.makeTableName(ryaInstance, queryId);
        // Fetch the Variable Orders for the binding sets and choose one of
        // them. It
        // doesn't matter which one we choose because they all result in the
        // same output.
        final PeriodicQueryStorageMetadata metadata = getPeriodicQueryMetadata(queryId);
        final VariableOrder varOrder = metadata.getVariableOrder();

        try {
            // Fetch only the Binding Sets whose Variable Order matches the
            // selected one.
            final Scanner scanner = accumuloConn.createScanner(tableName, auths);
            scanner.fetchColumnFamily(new Text(varOrder.toString()));
            if (binId.isPresent()) {
                scanner.setRange(Range.prefix(getRowPrefix(binId.get())));
            }
            return new AccumuloValueBindingSetIterator(scanner);

        } catch (final Exception e) {
            throw new PeriodicQueryStorageException(String.format("PCJ Table does not exist for name '%s'.", tableName), e);
        }
    }

    private Text getRowPrefix(final long binId) throws BindingSetConversionException {
        final QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding(PeriodicQueryResultStorage.PeriodicBinId, new LiteralImpl(Long.toString(binId), XMLSchema.LONG));

        return new Text(converter.convert(bs, new VariableOrder(PeriodicQueryResultStorage.PeriodicBinId)));
    }

    @Override
    public List<String> listPeriodicTables() {

        final List<String> periodicTables = new ArrayList<>();
        final String periodicPrefix = ryaInstance + PeriodicQueryTableNameFactory.PeriodicTableSuffix;
        boolean foundInstance = false;

        for (final String tableName : accumuloConn.tableOperations().list()) {
            if (tableName.startsWith(ryaInstance)) {
                // This table is part of the target Rya instance.
                foundInstance = true;

                if (tableName.startsWith(periodicPrefix)) {
                    periodicTables.add(tableName);
                }
            } else if (foundInstance) {
                // We have encountered the first table name that does not start
                // with the rya instance name after those that do. Because the
                // list is sorted, there can't be any more pcj tables for the
                // target instance in the list.
                break;
            }
        }
        return periodicTables;
    }

    /**
     * Class for removing any aggregate variables from the ProjectionElementList
     * of the parsed SPARQL queries. This ensures that only non-aggregation
     * values are contained in the Accumulo row.  The non-aggregation variables
     * are not updated while the aggregation variables are, so they are included in
     * the serialized BindingSet in the Accumulo Value field, which is overwritten
     * if an entry with the same Key and different Value (updated aggregation) is
     * written to the table.
     *
     */
    static class AggregateVariableRemover extends QueryModelVisitorBase<RuntimeException> {

        private Set<String> bindingNames;

        public Set<String> getNonAggregationVariables(final String sparql) throws MalformedQueryException {
            final TupleExpr te = new SPARQLParser().parseQuery(sparql, null).getTupleExpr();
            bindingNames = te.getBindingNames();
            te.visit(this);
            return bindingNames;
        }

        @Override
        public void meet(final ExtensionElem node) {
            if(node.getExpr() instanceof AggregateOperatorBase) {
                bindingNames.remove(node.getName());
            }
        }

    }

}
