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
package org.apache.rya.indexing.pcj.fluo.integration;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.log4j.Logger;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.indexing.pcj.fluo.api.CreateFluoPcj;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;

public class StreamingTestIT extends RyaExportITBase {

	private static final Logger log = Logger.getLogger(StreamingTestIT.class);

	@Test
	public void testRandomStreamingIngest() throws Exception {
	    final String sparql =
	            "select ?name ?uuid where { " +
                    "?uuid <http://pred1> ?name ; "  +
                    "<http://pred2> \"literal\"." +
                "}";

	    try (FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
	        // Create the PCJ table.
	        final Connector accumuloConn = super.getAccumuloConnector();
	        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName());
	        final String pcjId = pcjStorage.createPcj(sparql);

	        // Task the Fluo app with the PCJ.
	        new CreateFluoPcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, getRyaInstanceName());

	        // Add Statements to the Fluo app.
	        log.info("Adding Join Pairs...");
	        addRandomQueryStatementPairs(100);

	        super.getMiniFluo().waitForObservers();

	        int resultCount = 0;
	        try(CloseableIterator<BindingSet> resultsIt = pcjStorage.listResults(pcjId)) {
	            while(resultsIt.hasNext()) {
	                resultCount++;
	                resultsIt.next();
	            }
	        }

	        // Show the correct number of Binding Sets were created for the PCJ.
	        assertEquals(100, resultCount);
	    }
	}

	private void addRandomQueryStatementPairs(final int numPairs) throws Exception {
		final Set<Statement> statementPairs = new HashSet<>();
		for (int i = 0; i < numPairs; i++) {
			final String uri = "http://uuid_" + UUID.randomUUID().toString();
			final Statement statement1 = new StatementImpl(new URIImpl(uri), new URIImpl("http://pred1"),
					new LiteralImpl("number_" + (i + 1)));
			final Statement statement2 = new StatementImpl(new URIImpl(uri), new URIImpl("http://pred2"), new LiteralImpl("literal"));
			statementPairs.add(statement1);
			statementPairs.add(statement2);
		}
		super.getRyaSailRepository().getConnection().add(statementPairs, new Resource[0]);
		super.getMiniFluo().waitForObservers();
	}
}