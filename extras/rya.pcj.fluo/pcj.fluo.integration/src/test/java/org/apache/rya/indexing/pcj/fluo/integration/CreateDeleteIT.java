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

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Span;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.indexing.pcj.fluo.api.DeleteFluoPcj;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.sail.SailRepositoryConnection;

import com.google.common.collect.Sets;

/**
 * Tests that ensure the PCJ delete support works.
 */
public class CreateDeleteIT extends RyaExportITBase {

    @Test
    public void deletePCJ() throws Exception {
        // A query that finds people who talk to Eve and work at Chipotle.
        final String sparql =
                "SELECT ?x " + "WHERE { " +
                    "?x <http://talksTo> <http://Eve>. " +
                    "?x <http://worksAt> <http://Chipotle>." +
                "}";

        // Triples that are loaded into Rya before the PCJ is created.
        final ValueFactory vf = new ValueFactoryImpl();
        final Set<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://talksTo"), vf.createURI("http://Eve")),
                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://talksTo"), vf.createURI("http://Eve")),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://talksTo"), vf.createURI("http://Eve")),

                vf.createStatement(vf.createURI("http://Eve"), vf.createURI("http://helps"), vf.createURI("http://Kevin")),

                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")),
                vf.createStatement(vf.createURI("http://Eve"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")),
                vf.createStatement(vf.createURI("http://David"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadData(sparql, statements);

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Ensure the data was loaded.
            final List<Bytes> rows = getFluoTableEntries(fluoClient);
            assertEquals(20, rows.size());

            // Delete the PCJ from the Fluo application.
            new DeleteFluoPcj(1).deletePcj(fluoClient, pcjId);

            // Ensure all data related to the query has been removed.
            final List<Bytes> empty_rows = getFluoTableEntries(fluoClient);
            assertEquals(0, empty_rows.size());
        }
    }

    @Test
    public void deleteAggregation() throws Exception {
        // A query that finds the maximum price for an item within the inventory.
        final String sparql =
                "SELECT (max(?price) as ?maxPrice) { " +
                    "?item <urn:price> ?price . " +
                "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:price"), vf.createLiteral(2.50)),
                vf.createStatement(vf.createURI("urn:gum"), vf.createURI("urn:price"), vf.createLiteral(0.99)),
                vf.createStatement(vf.createURI("urn:sandwich"), vf.createURI("urn:price"), vf.createLiteral(4.99)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadData(sparql, statements);

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Ensure the data was loaded.
            final List<Bytes> rows = getFluoTableEntries(fluoClient);
            assertEquals(12, rows.size());

            // Delete the PCJ from the Fluo application.
            new DeleteFluoPcj(1).deletePcj(fluoClient, pcjId);

            // Ensure all data related to the query has been removed.
            final List<Bytes> empty_rows = getFluoTableEntries(fluoClient);
            assertEquals(0, empty_rows.size());
        }
    }
    

    private String loadData(final String sparql, final Collection<Statement> statements) throws Exception {
        requireNonNull(sparql);
        requireNonNull(statements);

        // Register the PCJ with Rya.
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(createConnectionDetails(), getAccumuloConnector());

        final String pcjId = ryaClient.getCreatePCJ().createPCJ(getRyaInstanceName(), sparql);

        // Write the data to Rya.
        final SailRepositoryConnection ryaConn = super.getRyaSailRepository().getConnection();
        ryaConn.begin();
        ryaConn.add(statements);
        ryaConn.commit();
        ryaConn.close();

        // Wait for the Fluo application to finish computing the end result.
        super.getMiniFluo().waitForObservers();

        // The PCJ Id is the topic name the results will be written to.
        return pcjId;
    }

    private List<Bytes> getFluoTableEntries(final FluoClient fluoClient) {
        try (Snapshot snapshot = fluoClient.newSnapshot()) {
            final List<Bytes> rows = new ArrayList<>();
            final RowScanner rscanner = snapshot.scanner().over(Span.prefix("")).byRow().build();

            for(final ColumnScanner cscanner: rscanner) {
            	rows.add(cscanner.getRow());
            }

            return rows;
        }
    }
}