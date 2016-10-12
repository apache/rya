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
import static java.util.Objects.requireNonNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.pcj.fluo.app.FluoStringConverter;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.SparqlFluoQueryBuilder;
import org.apache.rya.indexing.pcj.fluo.app.query.SparqlFluoQueryBuilder.NodeIds;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import info.aduna.iteration.CloseableIteration;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Transaction;

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
     * The default Statement Pattern batch insert size is 1000.
     */
    private static final int DEFAULT_SP_INSERT_BATCH_SIZE = 1000;

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
     * Tells the Fluo PCJ Updater application to maintain a new PCJ.
     * <p>
     * This call scans Rya for Statement Pattern matches and inserts them into
     * the Fluo application. The Fluo application will then maintain the intermediate
     * results as new triples are inserted and export any new query results to the
     * {@code pcjId} within the provided {@code pcjStorage}.
     *
     * @param pcjId - Identifies the PCJ that will be updated by the Fluo app. (not null)
     * @param pcjStorage - Provides access to the PCJ index. (not null)
     * @param fluo - A connection to the Fluo application that updates the PCJ index. (not null)
     * @param rya - A connection to the Rya instance hosting the PCJ, (not null)
     *
     * @throws MalformedQueryException The SPARQL query stored for the {@code pcjId} is malformed.
     * @throws PcjException The PCJ Metadata for {@code pcjId} could not be read from {@code pcjStorage}.
     * @throws SailException Historic PCJ results could not be loaded because of a problem with {@code rya}.
     * @throws QueryEvaluationException Historic PCJ results could not be loaded because of a problem with {@code rya}.
     */
    public void withRyaIntegration(
            final String pcjId,
            final PrecomputedJoinStorage pcjStorage,
            final FluoClient fluo,
            final SailRepository rya)
                    throws MalformedQueryException, PcjException, SailException, QueryEvaluationException {
        requireNonNull(pcjId);
        requireNonNull(pcjStorage);
        requireNonNull(fluo);
        requireNonNull(rya);

        // Keeps track of the IDs that are assigned to each of the query's nodes in Fluo.
        // We use these IDs later when scanning Rya for historic Statement Pattern matches
        // as well as setting up automatic exports.
        final NodeIds nodeIds = new NodeIds();

        // Parse the query's structure for the metadata that will be written to fluo.
        final PcjMetadata pcjMetadata = pcjStorage.getPcjMetadata(pcjId);
        final String sparql = pcjMetadata.getSparql();
        final ParsedQuery parsedQuery = new SPARQLParser().parseQuery(sparql, null);
        final FluoQuery fluoQuery = new SparqlFluoQueryBuilder().make(parsedQuery, nodeIds);

        try(Transaction tx = fluo.newTransaction()) {
            // Write the query's structure to Fluo.
            new FluoQueryMetadataDAO().write(tx, fluoQuery);

            // The results of the query are eventually exported to an instance of Rya, so store the Rya ID for the PCJ.
            final String queryId = fluoQuery.getQueryMetadata().getNodeId();
            tx.set(queryId, FluoQueryColumns.RYA_PCJ_ID, pcjId);
            tx.set(pcjId, FluoQueryColumns.PCJ_ID_QUERY_ID, queryId);
            
            // Flush the changes to Fluo.
            tx.commit();
        }

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

        try(Transaction tx = fluo.newTransaction()) {
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
                tx.set(spNodeId + NODEID_BS_DELIM + bindingSetStr, FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET, bindingSetStr);
            }

            tx.commit();
        }
    }
}
