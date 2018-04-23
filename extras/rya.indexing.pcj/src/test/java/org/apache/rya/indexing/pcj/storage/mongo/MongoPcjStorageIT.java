/**
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
package org.apache.rya.indexing.pcj.storage.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.ShiftVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.mongodb.MongoITBase;
import org.apache.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

/**
 * Integration tests the methods of {@link AccumuloPcjStorage}.
 * </p>
 * These tests ensures that the PCJ tables are maintained and that these operations
 * also update the Rya instance's details.
 */
public class MongoPcjStorageIT extends MongoITBase {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Test
    public void createPCJ() throws Exception {
        // Setup the PCJ storage that will be tested against.
        try(final PrecomputedJoinStorage pcjStorage =  new MongoPcjStorage(getMongoClient(), conf.getRyaInstanceName())) {
            // Create a PCJ.
            final MongoRyaInstanceDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(getMongoClient(), conf.getRyaInstanceName());
            detailsRepo.initialize(
                    RyaDetails.builder()
                    .setRyaInstanceName(conf.getRyaInstanceName())
                    .setRyaVersion("test")
                    .setEntityCentricIndexDetails(new EntityCentricIndexDetails(false))
                    .setTemporalIndexDetails(new TemporalIndexDetails(false))
                    .setFreeTextDetails(new FreeTextIndexDetails(false))
                    .setProspectorDetails(new ProspectorDetails(Optional.absent()))
                    .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.absent()))
                    .setPCJIndexDetails(PCJIndexDetails.builder().setEnabled(true))
                    .build()
                    );
            final String pcjId = pcjStorage.createPcj("SELECT * WHERE { ?a <http://isA> ?b } ");

            // Ensure the Rya details have been updated to include the PCJ's ID.
            final ImmutableMap<String, PCJDetails> detailsMap = detailsRepo.getRyaInstanceDetails()
                    .getPCJIndexDetails()
                    .getPCJDetails();

            final PCJDetails expectedDetails = PCJDetails.builder()
                    .setId( pcjId )
                    .build();

            assertEquals(expectedDetails, detailsMap.get(pcjId));
        }
    }


    @Test
    public void dropPCJ() throws Exception {
        try(final PrecomputedJoinStorage pcjStorage =  new MongoPcjStorage(getMongoClient(), conf.getRyaInstanceName())) {
            final MongoRyaInstanceDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(getMongoClient(), conf.getRyaInstanceName());
            detailsRepo.initialize(
                    RyaDetails.builder()
                    .setRyaInstanceName(conf.getRyaInstanceName())
                    .setRyaVersion("test")
                    .setEntityCentricIndexDetails(new EntityCentricIndexDetails(false))
                    .setTemporalIndexDetails(new TemporalIndexDetails(false))
                    .setFreeTextDetails(new FreeTextIndexDetails(false))
                    .setProspectorDetails(new ProspectorDetails(Optional.absent()))
                    .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.absent()))
                    .setPCJIndexDetails(PCJIndexDetails.builder().setEnabled(true))
                    .build()
                    );
            // Create a PCJ.
            final String pcjId = pcjStorage.createPcj("SELECT * WHERE { ?a <http://isA> ?b } ");

            // Delete the PCJ that was just created.
            pcjStorage.dropPcj(pcjId);

            // Ensure the Rya details have been updated to no longer include the PCJ's ID.

            final ImmutableMap<String, PCJDetails> detailsMap = detailsRepo.getRyaInstanceDetails()
                    .getPCJIndexDetails()
                    .getPCJDetails();

            assertFalse( detailsMap.containsKey(pcjId) );
        }
    }

    @Test
    public void listPcjs() throws Exception {
        try(final PrecomputedJoinStorage pcjStorage =  new MongoPcjStorage(getMongoClient(), conf.getRyaInstanceName())) {
            final MongoRyaInstanceDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(getMongoClient(), conf.getRyaInstanceName());
            detailsRepo.initialize(
                    RyaDetails.builder()
                    .setRyaInstanceName(conf.getRyaInstanceName())
                    .setRyaVersion("test")
                    .setEntityCentricIndexDetails(new EntityCentricIndexDetails(false))
                    .setTemporalIndexDetails(new TemporalIndexDetails(false))
                    .setFreeTextDetails(new FreeTextIndexDetails(false))
                    .setProspectorDetails(new ProspectorDetails(Optional.absent()))
                    .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.absent()))
                    .setPCJIndexDetails(PCJIndexDetails.builder().setEnabled(true))
                    .build()
                    );
            // Create a few PCJs and hold onto their IDs.
            final List<String> expectedIds = new ArrayList<>();

            String pcjId = pcjStorage.createPcj("SELECT * WHERE { ?a <http://isA> ?b } ");
            expectedIds.add( pcjId );

            pcjId = pcjStorage.createPcj("SELECT * WHERE { ?a <http://isA> ?b } ");
            expectedIds.add( pcjId );

            pcjId = pcjStorage.createPcj("SELECT * WHERE { ?a <http://isA> ?b } ");
            expectedIds.add( pcjId );

            // Fetch the PCJ names
            final List<String> pcjIds = pcjStorage.listPcjs();

            // Ensure the expected IDs match the fetched IDs.
            Collections.sort(expectedIds);
            Collections.sort(pcjIds);
            assertEquals(expectedIds, pcjIds);
        }
    }

    @Test
    public void getPcjMetadata() throws Exception {
        try(final PrecomputedJoinStorage pcjStorage =  new MongoPcjStorage(getMongoClient(), conf.getRyaInstanceName())) {
            final MongoRyaInstanceDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(getMongoClient(), conf.getRyaInstanceName());
            detailsRepo.initialize(
                    RyaDetails.builder()
                    .setRyaInstanceName(conf.getRyaInstanceName())
                    .setRyaVersion("test")
                    .setEntityCentricIndexDetails(new EntityCentricIndexDetails(false))
                    .setTemporalIndexDetails(new TemporalIndexDetails(false))
                    .setFreeTextDetails(new FreeTextIndexDetails(false))
                    .setProspectorDetails(new ProspectorDetails(Optional.absent()))
                    .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.absent()))
                    .setPCJIndexDetails(PCJIndexDetails.builder().setEnabled(true))
                    .build()
                    );
            // Create a PCJ.
            final String sparql = "SELECT * WHERE { ?a <http://isA> ?b }";
            final String pcjId = pcjStorage.createPcj(sparql);

            // Fetch the PCJ's metadata.
            final PcjMetadata metadata = pcjStorage.getPcjMetadata(pcjId);

            // Ensure it has the expected values.
            final Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(sparql);
            final PcjMetadata expectedMetadata = new PcjMetadata(sparql, 0L, varOrders);
            assertEquals(expectedMetadata, metadata);
        }
    }

    @Test
    public void addResults() throws Exception {
        try(final PrecomputedJoinStorage pcjStorage =  new MongoPcjStorage(getMongoClient(), conf.getRyaInstanceName())) {
            final MongoRyaInstanceDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(getMongoClient(), conf.getRyaInstanceName());
            detailsRepo.initialize(
                    RyaDetails.builder()
                    .setRyaInstanceName(conf.getRyaInstanceName())
                    .setRyaVersion("test")
                    .setEntityCentricIndexDetails(new EntityCentricIndexDetails(false))
                    .setTemporalIndexDetails(new TemporalIndexDetails(false))
                    .setFreeTextDetails(new FreeTextIndexDetails(false))
                    .setProspectorDetails(new ProspectorDetails(Optional.absent()))
                    .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.absent()))
                    .setPCJIndexDetails(PCJIndexDetails.builder().setEnabled(true))
                    .build()
                    );
            // Create a PCJ.
            final String sparql = "SELECT * WHERE { ?a <http://isA> ?b }";
            final String pcjId = pcjStorage.createPcj(sparql);

            // Add some binding sets to it.
            final Set<VisibilityBindingSet> results = new HashSet<>();

            final MapBindingSet aliceBS = new MapBindingSet();
            aliceBS.addBinding("a", VF.createIRI("http://Alice"));
            aliceBS.addBinding("b", VF.createIRI("http://Person"));
            results.add( new VisibilityBindingSet(aliceBS, "") );

            final MapBindingSet charlieBS = new MapBindingSet();
            charlieBS.addBinding("a", VF.createIRI("http://Charlie"));
            charlieBS.addBinding("b", VF.createIRI("http://Comedian"));
            results.add( new VisibilityBindingSet(charlieBS, "") );

            pcjStorage.addResults(pcjId, results);

            // Make sure the PCJ metadata was updated.
            final PcjMetadata metadata = pcjStorage.getPcjMetadata(pcjId);

            final Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(sparql);
            final PcjMetadata expectedMetadata = new PcjMetadata(sparql, 2L, varOrders);
            assertEquals(expectedMetadata, metadata);
        }
    }

    @Test
    public void listResults() throws Exception {
        try(final PrecomputedJoinStorage pcjStorage =  new MongoPcjStorage(getMongoClient(), conf.getRyaInstanceName())) {
            final MongoRyaInstanceDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(getMongoClient(), conf.getRyaInstanceName());
            detailsRepo.initialize(
                    RyaDetails.builder()
                    .setRyaInstanceName(conf.getRyaInstanceName())
                    .setRyaVersion("test")
                    .setEntityCentricIndexDetails(new EntityCentricIndexDetails(false))
                    .setTemporalIndexDetails(new TemporalIndexDetails(false))
                    .setFreeTextDetails(new FreeTextIndexDetails(false))
                    .setProspectorDetails(new ProspectorDetails(Optional.absent()))
                    .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.absent()))
                    .setPCJIndexDetails(PCJIndexDetails.builder().setEnabled(true))
                    .build()
                    );
            // Create a PCJ.
            final String sparql = "SELECT * WHERE { ?a <http://isA> ?b }";
            final String pcjId = pcjStorage.createPcj(sparql);

            // Add some binding sets to it.
            final Set<VisibilityBindingSet> visiSets = new HashSet<>();
            final Set<BindingSet> expectedResults = new HashSet<>();

            final MapBindingSet aliceBS = new MapBindingSet();
            aliceBS.addBinding("a", VF.createIRI("http://Alice"));
            aliceBS.addBinding("b", VF.createIRI("http://Person"));
            visiSets.add( new VisibilityBindingSet(aliceBS, "") );
            expectedResults.add(aliceBS);

            final MapBindingSet charlieBS = new MapBindingSet();
            charlieBS.addBinding("a", VF.createIRI("http://Charlie"));
            charlieBS.addBinding("b", VF.createIRI("http://Comedian"));
            visiSets.add( new VisibilityBindingSet(charlieBS, "") );
            expectedResults.add(charlieBS);

            pcjStorage.addResults(pcjId, visiSets);

            // List the results that were stored.
            final Set<BindingSet> results = new HashSet<>();
            try(CloseableIterator<BindingSet> resultsIt = pcjStorage.listResults(pcjId)) {
                while(resultsIt.hasNext()) {
                    results.add( resultsIt.next() );
                }
            }

            assertEquals(expectedResults, results);
        }
    }

    @Test
    public void purge() throws Exception {
        try(final PrecomputedJoinStorage pcjStorage =  new MongoPcjStorage(getMongoClient(), conf.getRyaInstanceName())) {
            final MongoRyaInstanceDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(getMongoClient(), conf.getRyaInstanceName());
            detailsRepo.initialize(
                    RyaDetails.builder()
                    .setRyaInstanceName(conf.getRyaInstanceName())
                    .setRyaVersion("test")
                    .setEntityCentricIndexDetails(new EntityCentricIndexDetails(false))
                    .setTemporalIndexDetails(new TemporalIndexDetails(false))
                    .setFreeTextDetails(new FreeTextIndexDetails(false))
                    .setProspectorDetails(new ProspectorDetails(Optional.absent()))
                    .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.absent()))
                    .setPCJIndexDetails(PCJIndexDetails.builder().setEnabled(true))
                    .build()
                    );
            // Create a PCJ.
            final String sparql = "SELECT * WHERE { ?a <http://isA> ?b }";
            final String pcjId = pcjStorage.createPcj(sparql);

            // Add some binding sets to it.
            final Set<VisibilityBindingSet> expectedResults = new HashSet<>();

            final MapBindingSet aliceBS = new MapBindingSet();
            aliceBS.addBinding("a", VF.createIRI("http://Alice"));
            aliceBS.addBinding("b", VF.createIRI("http://Person"));
            expectedResults.add( new VisibilityBindingSet(aliceBS, "") );

            final MapBindingSet charlieBS = new MapBindingSet();
            charlieBS.addBinding("a", VF.createIRI("http://Charlie"));
            charlieBS.addBinding("b", VF.createIRI("http://Comedian"));
            expectedResults.add( new VisibilityBindingSet(charlieBS, "") );

            pcjStorage.addResults(pcjId, expectedResults);

            // Purge the PCJ.
            pcjStorage.purge(pcjId);

            // List the results that were stored.
            final Set<BindingSet> results = new HashSet<>();
            try(CloseableIterator<BindingSet> resultsIt = pcjStorage.listResults(pcjId)) {
                while(resultsIt.hasNext()) {
                    results.add( resultsIt.next() );
                }
            }

            assertTrue( results.isEmpty() );

            // Make sure the PCJ metadata was updated.
            final PcjMetadata metadata = pcjStorage.getPcjMetadata(pcjId);

            final Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(sparql);
            final PcjMetadata expectedMetadata = new PcjMetadata(sparql, 0L, varOrders);
            assertEquals(expectedMetadata, metadata);
        }
    }
}