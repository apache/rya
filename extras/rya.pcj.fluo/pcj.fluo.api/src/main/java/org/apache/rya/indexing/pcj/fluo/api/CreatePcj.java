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

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Transaction;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.query.AccumuloRyaQueryEngine;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.BatchRyaQuery;
import org.apache.rya.api.resolver.RdfToRyaConversions;
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
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.sail.SailException;

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
     * @param queryEngine - QueryEngine for a given Rya Instance, (not null)
     *
     * @throws MalformedQueryException The SPARQL query stored for the {@code pcjId} is malformed.
     * @throws PcjException The PCJ Metadata for {@code pcjId} could not be read from {@code pcjStorage}.
     * @throws SailException Historic PCJ results could not be loaded because of a problem with {@code rya}.
     * @throws QueryEvaluationException Historic PCJ results could not be loaded because of a problem with {@code rya}.
     */
    public String  withRyaIntegration(final String pcjId, final PrecomputedJoinStorage pcjStorage, final FluoClient fluo,
            final Connector accumulo, String ryaInstance )
                    throws MalformedQueryException, PcjException, SailException, QueryEvaluationException, RyaDAOException {
        requireNonNull(pcjId);
        requireNonNull(pcjStorage);
        requireNonNull(fluo);
		requireNonNull(accumulo);
		requireNonNull(ryaInstance);
		
		//Create AccumuloRyaQueryEngine to query for historic results
		AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
		conf.setTablePrefix(ryaInstance);
		conf.setAuths(getAuths(accumulo));
		AccumuloRyaQueryEngine queryEngine = new AccumuloRyaQueryEngine(accumulo, conf);
		

		// Keeps track of the IDs that are assigned to each of the query's nodes
		// in Fluo.
		// We use these IDs later when scanning Rya for historic Statement
		// Pattern matches
		// as well as setting up automatic exports.
		final NodeIds nodeIds = new NodeIds();

		// Parse the query's structure for the metadata that will be written to
		// fluo.
		final PcjMetadata pcjMetadata = pcjStorage.getPcjMetadata(pcjId);
		final String sparql = pcjMetadata.getSparql();
		final ParsedQuery parsedQuery = new SPARQLParser().parseQuery(sparql, null);
		final FluoQuery fluoQuery = new SparqlFluoQueryBuilder().make(parsedQuery, nodeIds);

        // return queryId to the caller for later monitoring from the export.
        String queryId = null;

		try (Transaction tx = fluo.newTransaction()) {
			// Write the query's structure to Fluo.
			new FluoQueryMetadataDAO().write(tx, fluoQuery);

			// The results of the query are eventually exported to an instance
			// of Rya, so store the Rya ID for the PCJ.
            queryId = fluoQuery.getQueryMetadata().getNodeId();
			tx.set(queryId, FluoQueryColumns.RYA_PCJ_ID, pcjId);
			tx.set(pcjId, FluoQueryColumns.PCJ_ID_QUERY_ID, queryId);

			// Flush the changes to Fluo.
			tx.commit();
		}

		// Reuse the same set object while performing batch inserts.
		final Set<RyaStatement> queryBatch = new HashSet<>();

		// Iterate through each of the statement patterns and insert their
		// historic matches into Fluo.
		for (final StatementPatternMetadata patternMetadata : fluoQuery.getStatementPatternMetadata()) {
			// Get an iterator over all of the binding sets that match the
			// statement pattern.
			final StatementPattern pattern = FluoStringConverter
					.toStatementPattern(patternMetadata.getStatementPattern());
			queryBatch.add(spToRyaStatement(pattern));
		}

		Iterator<RyaStatement> triples = queryEngine.query(new BatchRyaQuery(queryBatch)).iterator();
		Set<RyaStatement> triplesBatch = new HashSet<>();

		// Insert batches of the binding sets into Fluo.
		while (triples.hasNext()) {
			if (triplesBatch.size() == spInsertBatchSize) {
				writeBatch(fluo, triplesBatch);
				triplesBatch.clear();
			}

			triplesBatch.add(triples.next());
		}

		if (!triplesBatch.isEmpty()) {
			writeBatch(fluo, triplesBatch);
			triplesBatch.clear();
		}
        return queryId;
	}
    
    
    private static void writeBatch(final FluoClient fluo, final Set<RyaStatement> batch) {
        checkNotNull(fluo);
        checkNotNull(batch);
        
        new InsertTriples().insert(fluo, batch);

    }
    
    
    private static RyaStatement spToRyaStatement(StatementPattern sp) {
    
    	Value subjVal = sp.getSubjectVar().getValue();
    	Value predVal = sp.getPredicateVar().getValue();
    	Value objVal = sp.getObjectVar().getValue();
    	
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
    
    
    private String[] getAuths(Connector accumulo) {
   	 Authorizations auths;
		try {
			auths = accumulo.securityOperations().getUserAuthorizations(accumulo.whoami());
			List<byte[]> authList = auths.getAuthorizations();
	         String[] authArray = new String[authList.size()];
	         for(int i = 0; i < authList.size(); i++){
	         	authArray[i] = new String(authList.get(i), "UTF-8");
	         }
	         return authArray;
		} catch (AccumuloException | AccumuloSecurityException | UnsupportedEncodingException e) {
			throw new RuntimeException("Cannot read authorizations for user: " + accumulo.whoami());
		}
   }
    
    
}
