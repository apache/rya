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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Transaction;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.query.AccumuloRyaQueryEngine;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.BatchRyaQuery;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.pcj.fluo.app.FluoStringConverter;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.SparqlFluoQueryBuilder;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.calrissian.mango.collect.CloseableIterable;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

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
@DefaultAnnotation(NonNull.class)
public class CreateFluoPcj {
    private static final Logger log = Logger.getLogger(CreateFluoPcj.class);

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
     * Constructs an instance of {@link CreateFluoPcj} that uses
     * {@link #DEFAULT_SP_INSERT_BATCH_SIZE} as the default batch insert size.
     */
    public CreateFluoPcj() {
        this(DEFAULT_SP_INSERT_BATCH_SIZE);
    }

    /**
     * Constructs an instance of {@link CreateFluoPcj}.
     *
     * @param spInsertBatchSize - The maximum number of binding sets that will be
     *   inserted into each Statement Pattern's result set per Fluo transaction.
     */
    public CreateFluoPcj(final int spInsertBatchSize) {
        checkArgument(spInsertBatchSize > 0, "The SP insert batch size '" + spInsertBatchSize + "' must be greater than 0.");
        this.spInsertBatchSize = spInsertBatchSize;
    }
    

    /**
     * Tells the Fluo PCJ Updater application to maintain a new PCJ. This method
     * creates the FluoQuery (metadata) inside of Fluo so that results can be incrementally generated
     * inside of Fluo.  This method assumes that the user will export the results to Kafka or
     * some other external resource.  The export id is equivalent to the queryId that is returned,
     * which is in contrast to the other createPcj methods in this class which accept an external pcjId
     * that is used to identify the Accumulo table or Kafka topic for exporting results.
     *
     * @param sparql - sparql query String to be registered with Fluo
     * @param fluo - A connection to the Fluo application that updates the PCJ index. (not null)
     * @return The metadata that was written to the Fluo application for the PCJ.
     * @throws MalformedQueryException The SPARQL query stored for the {@code pcjId} is malformed.
     * @throws PcjException The PCJ Metadata for {@code pcjId} could not be read from {@code pcjStorage}.
     */
    public FluoQuery createPcj(String sparql, FluoClient fluo) throws MalformedQueryException {
        Preconditions.checkNotNull(sparql);
        Preconditions.checkNotNull(fluo);
        
        String pcjId = UUID.randomUUID().toString().replaceAll("-", "");
        return createPcj(pcjId, sparql, fluo);
    }
    
    /**
     * Tells the Fluo PCJ Updater application to maintain a new PCJ.  This method provides
     * no guarantees that a PCJ with the given pcjId exists outside of Fluo. This method merely
     * creates the FluoQuery (metadata) inside of Fluo so that results and be incrementally generated
     * inside of Fluo.  This method assumes that the user will export the results to Kafka or
     * some other external resource.
     *
     * @param pcjId - Identifies the PCJ that will be updated by the Fluo app. (not null)
     * @param sparql - sparql query String to be registered with Fluo
     * @param fluo - A connection to the Fluo application that updates the PCJ index. (not null)
     * @return The metadata that was written to the Fluo application for the PCJ.
     * @throws PcjException The PCJ Metadata for {@code pcjId} could not be read from {@code pcjStorage}.
     */
    public FluoQuery createPcj(
            final String pcjId,
            final String sparql,
            final FluoClient fluo) throws MalformedQueryException {
        requireNonNull(pcjId);
        requireNonNull(sparql);
        requireNonNull(fluo);

        FluoQuery fluoQuery = makeFluoQuery(sparql, pcjId);
        writeFluoQuery(fluo, fluoQuery, pcjId);

        return fluoQuery;
    }
    
    private FluoQuery makeFluoQuery(String sparql, String pcjId) throws MalformedQueryException {
        
        String queryId = NodeType.generateNewIdForType(NodeType.QUERY, pcjId);
        
        SparqlFluoQueryBuilder builder = new SparqlFluoQueryBuilder();
        builder.setFluoQueryId(queryId);
        builder.setSparql(sparql);
        
        return builder.build();
    }
    
    private void writeFluoQuery(FluoClient fluo, FluoQuery fluoQuery, String pcjId) {
        try (Transaction tx = fluo.newTransaction()) {
            // Write the query's structure to Fluo.
            new FluoQueryMetadataDAO().write(tx, fluoQuery);
            
            // Flush the changes to Fluo.
            tx.commit();
        }
    }

    
    /**
     * Tells the Fluo PCJ Updater application to maintain a new PCJ.  The method takes in an
     * instance of {@link PrecomputedJoinStorage} to verify that a PCJ with the given pcjId exists.
     *
     * @param pcjId - Identifies the PCJ that will be updated by the Fluo app. (not null)
     * @param pcjStorage - Provides access to the PCJ index. (not null)
     * @param fluo - A connection to the Fluo application that updates the PCJ index. (not null)
     * @return The metadata that was written to the Fluo application for the PCJ.
     * @throws MalformedQueryException The SPARQL query stored for the {@code pcjId} is malformed.
     * @throws PcjException The PCJ Metadata for {@code pcjId} could not be read from {@code pcjStorage}.
     */
    public FluoQuery createPcj(
            final String pcjId,
            final PrecomputedJoinStorage pcjStorage,
            final FluoClient fluo) throws MalformedQueryException, PcjException {
        requireNonNull(pcjId);
        requireNonNull(pcjStorage);
        requireNonNull(fluo);

        // Parse the query's structure for the metadata that will be written to fluo.
        final PcjMetadata pcjMetadata = pcjStorage.getPcjMetadata(pcjId);
        final String sparql = pcjMetadata.getSparql();
        return createPcj(pcjId, sparql, fluo);
    }
    
    /**
     * Tells the Fluo PCJ Updater application to maintain a new PCJ.
     * <p>
     * This call scans Rya for Statement Pattern matches and inserts them into
     * the Fluo application. This method does not verify that a PcjTable with the
     * the given pcjId actually exists. It is assumed that results for any query registered
     * using this method will be exported to Kafka or some other external service.
     *
     * @param pcjId - Identifies the PCJ that will be updated by the Fluo app. (not null)
     * @param sparql - sparql query that will registered with Fluo. (not null)
     * @param fluo - A connection to the Fluo application that updates the PCJ index. (not null)
     * @param queryEngine - QueryEngine for a given Rya Instance, (not null)
     * @return The Fluo application's Query ID of the query that was created.
     * @throws MalformedQueryException The SPARQL query stored for the {@code pcjId} is malformed.
     * @throws PcjException The PCJ Metadata for {@code pcjId} could not be read from {@code pcjStorage}.
     * @throws RyaDAOException Historic PCJ results could not be loaded because of a problem with {@code rya}.
     */
    public String withRyaIntegration(
            final String pcjId,
            final String sparql,
            final FluoClient fluo,
            final Connector accumulo,
            final String ryaInstance ) throws MalformedQueryException, PcjException, RyaDAOException {
        requireNonNull(pcjId);
        requireNonNull(sparql);
        requireNonNull(fluo);
        requireNonNull(accumulo);
        requireNonNull(ryaInstance);

        
        // Write the SPARQL query's structure to the Fluo Application.
        final FluoQuery fluoQuery = createPcj(pcjId, sparql, fluo);
        //import results already ingested into Rya that match query
        importHistoricResultsIntoFluo(fluo, fluoQuery, accumulo, ryaInstance);
        // return queryId to the caller for later monitoring from the export.
        return fluoQuery.getQueryMetadata().getNodeId();
    }
    

    /**
     * Tells the Fluo PCJ Updater application to maintain a new PCJ.
     * <p>
     * This call scans Rya for Statement Pattern matches and inserts them into
     * the Fluo application. The Fluo application will then maintain the intermediate
     * results as new triples are inserted and export any new query results to the
     * {@code pcjId} within the provided {@code pcjStorage}.  This method requires that a
     * PCJ table already exist for the query corresponding to the pcjId.  Results will be exported
     * to this table.
     *
     * @param pcjId - Identifies the PCJ that will be updated by the Fluo app. (not null)
     * @param pcjStorage - Provides access to the PCJ index. (not null)
     * @param fluo - A connection to the Fluo application that updates the PCJ index. (not null)
     * @param queryEngine - QueryEngine for a given Rya Instance, (not null)
     * @return The Fluo application's Query ID of the query that was created.
     * @throws MalformedQueryException The SPARQL query stored for the {@code pcjId} is malformed.
     * @throws PcjException The PCJ Metadata for {@code pcjId} could not be read from {@code pcjStorage}.
     * @throws RyaDAOException Historic PCJ results could not be loaded because of a problem with {@code rya}.
     */
    public String withRyaIntegration(
            final String pcjId,
            final PrecomputedJoinStorage pcjStorage,
            final FluoClient fluo,
            final Connector accumulo,
            final String ryaInstance ) throws MalformedQueryException, PcjException, RyaDAOException {
        requireNonNull(pcjId);
        requireNonNull(pcjStorage);
        requireNonNull(fluo);
        requireNonNull(accumulo);
        requireNonNull(ryaInstance);
        
        // Parse the query's structure for the metadata that will be written to fluo.
        final PcjMetadata pcjMetadata = pcjStorage.getPcjMetadata(pcjId);
        final String sparql = pcjMetadata.getSparql();
        
        return withRyaIntegration(pcjId, sparql, fluo, accumulo, ryaInstance);
    }
    
    private void importHistoricResultsIntoFluo(FluoClient fluo, FluoQuery fluoQuery, Connector accumulo, String ryaInstance)
            throws RyaDAOException {
        // Reuse the same set object while performing batch inserts.
        final Set<RyaStatement> queryBatch = new HashSet<>();

        // Iterate through each of the statement patterns and insert their
        // historic matches into Fluo.
        for (final StatementPatternMetadata patternMetadata : fluoQuery.getStatementPatternMetadata()) {
            // Get an iterator over all of the binding sets that match the
            // statement pattern.
            final StatementPattern pattern = FluoStringConverter.toStatementPattern(patternMetadata.getStatementPattern());
            queryBatch.add(spToRyaStatement(pattern));
        }

        // Create AccumuloRyaQueryEngine to query for historic results
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(ryaInstance);
        conf.setAuths(getAuths(accumulo));

        try (final AccumuloRyaQueryEngine queryEngine = new AccumuloRyaQueryEngine(accumulo, conf);
                CloseableIterable<RyaStatement> queryIterable = queryEngine.query(new BatchRyaQuery(queryBatch))) {
            final Set<RyaStatement> triplesBatch = new HashSet<>();

            // Insert batches of the binding sets into Fluo.
            for (final RyaStatement ryaStatement : queryIterable) {
                if (triplesBatch.size() == spInsertBatchSize) {
                    writeBatch(fluo, triplesBatch);
                    triplesBatch.clear();
                }

                triplesBatch.add(ryaStatement);
            }

            if (!triplesBatch.isEmpty()) {
                writeBatch(fluo, triplesBatch);
                triplesBatch.clear();
            }
        } catch (final IOException e) {
            log.warn("Ignoring IOException thrown while closing the AccumuloRyaQueryEngine used by CreatePCJ.", e);
        }
    }

    private static void writeBatch(final FluoClient fluo, final Set<RyaStatement> batch) {
        checkNotNull(fluo);
        checkNotNull(batch);
        new InsertTriples().insert(fluo, batch);
    }

    private static RyaStatement spToRyaStatement(final StatementPattern sp) {
        final Value subjVal = sp.getSubjectVar().getValue();
        final Value predVal = sp.getPredicateVar().getValue();
        final Value objVal = sp.getObjectVar().getValue();

        RyaURI subjURI = null;
        RyaURI predURI = null;
        RyaType objType = null;

        if(subjVal != null) {
            if(!(subjVal instanceof Resource)) {
                throw new AssertionError("Subject must be a Resource.");
            }
            subjURI = RdfToRyaConversions.convertResource((Resource) subjVal);
        }

        if (predVal != null) {
            if(!(predVal instanceof URI)) {
                throw new AssertionError("Predicate must be a URI.");
            }
            predURI = RdfToRyaConversions.convertURI((URI) predVal);
        }

        if (objVal != null ) {
            objType = RdfToRyaConversions.convertValue(objVal);
        }

        return new RyaStatement(subjURI, predURI, objType);
    }

    private String[] getAuths(final Connector accumulo) {
        Authorizations auths;
        try {
            auths = accumulo.securityOperations().getUserAuthorizations(accumulo.whoami());
            final List<byte[]> authList = auths.getAuthorizations();
            final String[] authArray = new String[authList.size()];
            for(int i = 0; i < authList.size(); i++){
                authArray[i] = new String(authList.get(i), "UTF-8");
            }
            return authArray;
        } catch (AccumuloException | AccumuloSecurityException | UnsupportedEncodingException e) {
            throw new RuntimeException("Cannot read authorizations for user: " + accumulo.whoami());
        }
    }
}