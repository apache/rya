/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.api.client.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.Set;

import org.apache.rya.api.client.CreatePCJ;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import org.apache.rya.indexing.pcj.fluo.api.ListQueryIds;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * Integration tests the methods of {@link AccumuloCreatePCJ}.
 */
public class AccumuloCreatePCJIT extends FluoITBase {

    @Test
    public void createPCJ() throws Exception {
        AccumuloConnectionDetails connectionDetails = createConnectionDetails();
        // Initialize the commands that will be used by this test.
        final CreatePCJ createPCJ = new AccumuloCreatePCJ(connectionDetails, accumuloConn);

        // Create a PCJ.
        final String sparql =
                "SELECT ?x " +
                  "WHERE { " +
                  "?x <http://talksTo> <http://Eve>. " +
                  "?x <http://worksAt> <http://TacoJoint>." +
                "}";
        final String pcjId = createPCJ.createPCJ(getRyaInstanceName(), sparql);

        // Verify the RyaDetails were updated to include the new PCJ.
        final Optional<RyaDetails> ryaDetails = new AccumuloGetInstanceDetails(connectionDetails, accumuloConn).getDetails(getRyaInstanceName());
        final PCJDetails pcjDetails = ryaDetails.get().getPCJIndexDetails().getPCJDetails().get(pcjId);

        assertEquals(pcjId, pcjDetails.getId());
        assertFalse( pcjDetails.getLastUpdateTime().isPresent() );
        assertEquals(PCJUpdateStrategy.INCREMENTAL, pcjDetails.getUpdateStrategy().get());

        // Verify the PCJ's metadata was initialized.

        try(final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName())) {
            final PcjMetadata pcjMetadata = pcjStorage.getPcjMetadata(pcjId);
            assertEquals(sparql, pcjMetadata.getSparql());
            assertEquals(0L, pcjMetadata.getCardinality());


            // Verify a Query ID was added for the query within the Fluo app.
            final List<String> fluoQueryIds = new ListQueryIds().listQueryIds(fluoClient);
            assertEquals(1, fluoQueryIds.size());

            // Insert some statements into Rya.
            final ValueFactory vf = ryaRepo.getValueFactory();
            ryaConn.add(vf.createIRI("http://Alice"), vf.createIRI("http://talksTo"), vf.createIRI("http://Eve"));
            ryaConn.add(vf.createIRI("http://Bob"), vf.createIRI("http://talksTo"), vf.createIRI("http://Eve"));
            ryaConn.add(vf.createIRI("http://Charlie"), vf.createIRI("http://talksTo"), vf.createIRI("http://Eve"));

            ryaConn.add(vf.createIRI("http://Eve"), vf.createIRI("http://helps"), vf.createIRI("http://Kevin"));

            ryaConn.add(vf.createIRI("http://Bob"), vf.createIRI("http://worksAt"), vf.createIRI("http://TacoJoint"));
            ryaConn.add(vf.createIRI("http://Charlie"), vf.createIRI("http://worksAt"), vf.createIRI("http://TacoJoint"));
            ryaConn.add(vf.createIRI("http://Eve"), vf.createIRI("http://worksAt"), vf.createIRI("http://TacoJoint"));
            ryaConn.add(vf.createIRI("http://David"), vf.createIRI("http://worksAt"), vf.createIRI("http://TacoJoint"));

            // Verify the correct results were exported.
            fluo.waitForObservers();

            final Set<BindingSet> results = Sets.newHashSet( pcjStorage.listResults(pcjId) );

            final MapBindingSet bob = new MapBindingSet();
            bob.addBinding("x", vf.createIRI("http://Bob"));

            final MapBindingSet charlie = new MapBindingSet();
            charlie.addBinding("x", vf.createIRI("http://Charlie"));

            final Set<BindingSet> expected = Sets.newHashSet(bob, charlie);

            assertEquals(expected, results);
        }
    }

    @Test(expected = InstanceDoesNotExistException.class)
    public void createPCJ_instanceDoesNotExist() throws InstanceDoesNotExistException, RyaClientException {
        // Create a PCJ for a Rya instance that doesn't exist.
        final CreatePCJ createPCJ = new AccumuloCreatePCJ(createConnectionDetails(), accumuloConn);
        createPCJ.createPCJ("invalidInstanceName", "SELECT * where { ?a ?b ?c }");
    }

    @Test(expected = RyaClientException.class)
    public void createPCJ_invalidSparql() throws DuplicateInstanceNameException, RyaClientException {
        // Install an instance of Rya.
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(true)
                .setEnableEntityCentricIndex(false)
                .setEnableFreeTextIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(true)
                .setEnableGeoIndex(false)
                .setFluoPcjAppName(getRyaInstanceName())
                .build();

        final Install install = new AccumuloInstall(createConnectionDetails(), accumuloConn);
        install.install(getRyaInstanceName(), installConfig);

        // Create a PCJ using invalid SPARQL.
        final CreatePCJ createPCJ = new AccumuloCreatePCJ(createConnectionDetails(), accumuloConn);
        createPCJ.createPCJ(getRyaInstanceName(), "not valid sparql");
    }
}