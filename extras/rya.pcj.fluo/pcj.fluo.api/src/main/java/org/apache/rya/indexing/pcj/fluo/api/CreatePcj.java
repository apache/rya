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
package org.apache.rya.indexing.pcj.fluo.api;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.indexing.pcj.fluo.app.FluoStringConverter;
import org.apache.rya.indexing.pcj.fluo.app.StringTypeLayer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.SparqlFluoQueryBuilder;
import org.apache.rya.indexing.pcj.fluo.app.query.SparqlFluoQueryBuilder.NodeIds;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.ShiftVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import info.aduna.iteration.CloseableIteration;
import io.fluo.api.client.FluoClient;
import io.fluo.api.types.TypedTransaction;
import mvm.rya.rdftriplestore.RyaSailRepository;

/**
 * Sets up a new Pre Computed Join (PCJ) in Fluo from a SPARQL query.
 * <p>
 * This is a two phase process.
 * <ol>
 *   <li>Setup metadata about each node of the query using a single Fluo transaction. </li>
 *   <li>Scan Rya for binding sets that match each Statement Pattern from the query
 *       and use a separate Fluo transaction for each batch that is inserted. This
 *       ensure historic triples will be included in the query's results.</li>
 * </ol>
 * After the first step is finished, any new Triples that are added to the Fluo
 * application will be matched against statement patterns, the final results
 * will percolate to the top of the query, and those results will be exported to
 * Rya's query system.
 */
@ParametersAreNonnullByDefault
public class CreatePcj {

    /**
     * Wraps Fluo {@link Transaction}s so that we can write String values to them.
     */
    private static final StringTypeLayer STRING_TYPED_LAYER = new StringTypeLayer();

    /**
     * The default Statement Pattern batch insert size is 1000.
     */
    private static final int DEFAULT_SP_INSERT_BATCH_SIZE = 1000;

    /**
     * A utility used to interact with Rya's PCJ tables.
     */
    private static final PcjTables PCJ_TABLES = new PcjTables();

    /**
     * The maximum number of binding sets that will be inserted into each Statement
     * Pattern's result set per Fluo transaction.
     */
    private final int spInsertBatchSize;

    /**
     * Constructs an instance of {@link CreatePcj} that uses
     * {@link #DEFAULT_SP_INSERT_BATCH_SIZE} as the default batch insert size.
     */
    public CreatePcj() {
        this(DEFAULT_SP_INSERT_BATCH_SIZE);
    }

    /**
     * Constructs an instance of {@link CreatePcj}.
     *
     * @param spInsertBatchSize - The maximum number of binding sets that will be
     *   inserted into each Statement Pattern's result set per Fluo transaction.
     */
    public CreatePcj(final int spInsertBatchSize) {
        checkArgument(spInsertBatchSize > 0, "The SP insert batch size '" + spInsertBatchSize + "' must be greater than 0.");
        this.spInsertBatchSize = spInsertBatchSize;
    }

    /**
     * Sets up a new Pre Computed Join (PCJ) in Fluo from a SPARQL query. Historic
     * triples will be scanned and matched using the rya connection that was
     * provided. The PCJ will also automatically export to a table in Accumulo
     * named using the {@code ryaTablePrefix} and the query's ID from the Fluo table.
     *
     * @param fluo - A connection to the Fluo table that will be updated. (not null)
     * @param ryaTablePrefix - The prefix that will be prepended to the Accumulo table
     *   the PCJ's results will be exported to. (not null)
     * @param rya - A connection to the Rya repository that will be scanned. (not null)
     * @param accumuloConn - A connectino to the Accumulo instance the incremental
     *   results will be exported to as a Rya PCJ table. (not null)
     * @param varOrders - The variable orders the query's results will be exported to
     *   within the export table. If this set is empty, then a default will be
     *   used instead.(not null)
     * @param sparql - The SPARQL query whose results will be incrementally updated by Fluo. (not null)
     * @throws MalformedQueryException The PCJ could not be initialized because the SPARQL query was malformed.
     * @throws PcjException The PCJ could not be initialized because of a problem setting up the export location.
     * @throws SailException Historic results could not be added to the initialized PCJ because of
     *   a problem with the Rya connection.
     * @throws QueryEvaluationException Historic results could not be added to the initialized PCJ because of
     *   a problem with the Rya connection.
     */
    public void withRyaIntegration(
            final FluoClient fluo,
            final String ryaTablePrefix,
            final RyaSailRepository rya,
            final Connector accumuloConn,
            final Set<VariableOrder> varOrders,
            final String sparql) throws MalformedQueryException, PcjException, SailException, QueryEvaluationException {
        checkNotNull(fluo);
        checkNotNull(ryaTablePrefix);
        checkNotNull(rya);
        checkNotNull(accumuloConn);
        checkNotNull(varOrders);
        checkNotNull(sparql);

        // Parse the SPARQL into a POJO.
        final SPARQLParser parser = new SPARQLParser();
        final ParsedQuery parsedQuery = parser.parseQuery(sparql, null);

        // Keeps track of the IDs that are assigned to each of the query's nodes in Fluo.
        // We use these IDs later when scanning Rya for historic Statement Pattern matches
        // as well as setting up automatic exports.
        final NodeIds nodeIds = new NodeIds();
        final String exportTableName;
        final String queryId;

        // Parse the query's structure for the metadata that will be written to fluo.
        final FluoQuery fluoQuery = new SparqlFluoQueryBuilder().make(parsedQuery, nodeIds);

        try(TypedTransaction tx = STRING_TYPED_LAYER.wrap( fluo.newTransaction() )) {
            // Write the query's structure to Fluo.
            new FluoQueryMetadataDAO().write(tx, fluoQuery);

            // Since we are exporting the query's results to a table in Accumulo, store that location in the fluo table.
            queryId = fluoQuery.getQueryMetadata().getNodeId();

            exportTableName = new PcjTableNameFactory().makeTableName(ryaTablePrefix, queryId);
            tx.mutate().row(queryId).col(FluoQueryColumns.QUERY_RYA_EXPORT_TABLE_NAME).set(exportTableName);

            // Flush the changes to Fluo.
            tx.commit();
        }

        // Initialize the export destination in Accumulo. If triples are being written to Fluo
        // while this query is being created, then the export observer may throw errors for a while
        // until this step is completed.
        final VariableOrder queryVarOrder = fluoQuery.getQueryMetadata().getVariableOrder();
        if(varOrders.isEmpty()) {
            final Set<VariableOrder> shiftVarOrders = new ShiftVarOrderFactory().makeVarOrders( queryVarOrder );
            varOrders.addAll(shiftVarOrders);
        }
        PCJ_TABLES.createPcjTable(accumuloConn, exportTableName, varOrders, sparql);

        // Get a connection to Rya. It's used to scan for Statement Pattern results.
        final SailConnection ryaConn = rya.getSail().getConnection();

        // Reuse the same set object while performing batch inserts.
        final Set<BindingSet> batch = new HashSet<>();

        // Iterate through each of the statement patterns and insert their historic matches into Fluo.
        for(final StatementPatternMetadata patternMetadata : fluoQuery.getStatementPatternMetadata()) {
            // Get an iterator over all of the binding sets that match the statement pattern.
            final StatementPattern pattern = FluoStringConverter.toStatementPattern( patternMetadata.getStatementPattern() );
            final CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingSets = ryaConn.evaluate(pattern, null, null, false);

            // Insert batches of the binding sets into Fluo.
            while(bindingSets.hasNext()) {
                if(batch.size() == spInsertBatchSize) {
                    writeBatch(fluo, patternMetadata, batch);
                    batch.clear();
                }

                batch.add( bindingSets.next() );
            }

            if(!batch.isEmpty()) {
                writeBatch(fluo, patternMetadata, batch);
                batch.clear();
            }
        }
    }

    /**
     * Writes a batch of {@link BindingSet}s that match a statement pattern to Fluo.
     *
     * @param fluo - Creates transactions to Fluo. (not null)
     * @param spMetadata - The Statement Pattern the batch matches. (not null)
     * @param batch - A set of binding sets that are the result of the statement pattern. (not null)
     */
    private static void writeBatch(final FluoClient fluo, final StatementPatternMetadata spMetadata, final Set<BindingSet> batch) {
        checkNotNull(fluo);
        checkNotNull(spMetadata);
        checkNotNull(batch);

        final BindingSetStringConverter converter = new BindingSetStringConverter();

        try(TypedTransaction tx = STRING_TYPED_LAYER.wrap(fluo.newTransaction())) {
            // Get the node's variable order.
            final String spNodeId = spMetadata.getNodeId();
            final VariableOrder varOrder = spMetadata.getVariableOrder();

            for(final BindingSet bindingSet : batch) {
                final MapBindingSet spBindingSet = new MapBindingSet();
                for(final String var : varOrder) {
                    final Binding binding = bindingSet.getBinding(var);
                    spBindingSet.addBinding(binding);
                }

                final String bindingSetStr = converter.convert(spBindingSet, varOrder);

                // Write the binding set entry to Fluo for the statement pattern.
                tx.mutate().row(spNodeId + NODEID_BS_DELIM + bindingSetStr)
                    .col(FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET)
                    .set(bindingSetStr);
            }

            tx.commit();
        }
    }
}