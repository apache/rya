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
package org.apache.rya.api.client.accumulo;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.rya.api.client.CreatePCJ;
import org.apache.rya.api.client.DeletePCJ;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.indexing.pcj.fluo.api.ListQueryIds;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests the methods of {@link AccumuloCreatePCJ}.
 */
public class AccumuloDeletePCJIT extends FluoITBase {

    @Test
    public void deletePCJ() throws RyaClientException, PCJStorageException, RepositoryException {
        // Initialize the commands that will be used by this test.
        final CreatePCJ createPCJ = new AccumuloCreatePCJ(createConnectionDetails(), accumuloConn);

        // Create a PCJ.
        final String sparql =
                "SELECT ?x " +
                  "WHERE { " +
                  "?x <http://talksTo> <http://Eve>. " +
                  "?x <http://worksAt> <http://TacoJoint>." +
                "}";
        final String pcjId = createPCJ.createPCJ(getRyaInstanceName(), sparql);

        // Verify a Query ID was added for the query within the Fluo app.
        List<String> fluoQueryIds = new ListQueryIds().listQueryIds(fluoClient);
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


        try(final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName())) {
            final Set<BindingSet> results = Sets.newHashSet( pcjStorage.listResults(pcjId) );

            final MapBindingSet bob = new MapBindingSet();
            bob.addBinding("x", vf.createIRI("http://Bob"));

            final MapBindingSet charlie = new MapBindingSet();
            charlie.addBinding("x", vf.createIRI("http://Charlie"));

            final Set<BindingSet> expected = Sets.newHashSet(bob, charlie);
            assertEquals(expected, results);


            // Delete the PCJ.
            final DeletePCJ deletePCJ = new AccumuloDeletePCJ(createConnectionDetails(), accumuloConn);
            deletePCJ.deletePCJ(getRyaInstanceName(), pcjId);

            // Ensure the PCJ's metadata has been removed from the storage.
            assertTrue( pcjStorage.listPcjs().isEmpty() );

            // Ensure the PCJ has been removed from the Fluo application.
            fluo.waitForObservers();

            // Verify a Query ID was added for the query within the Fluo app.
            fluoQueryIds = new ListQueryIds().listQueryIds(fluoClient);
            assertEquals(0, fluoQueryIds.size());
        }
    }

    @Test(expected = InstanceDoesNotExistException.class)
    public void deletePCJ_instanceDoesNotExist() throws RyaClientException {
        // Delete a PCJ for a Rya instance that doesn't exist.
        final DeletePCJ deletePCJ = new AccumuloDeletePCJ(createConnectionDetails(), accumuloConn);
        deletePCJ.deletePCJ("doesNotExist", "randomID");
    }

    @Test(expected = RyaClientException.class)
    public void deletePCJ_pcjDoesNotExist() throws RyaClientException {
        // Delete the PCJ.
        final DeletePCJ deletePCJ = new AccumuloDeletePCJ(createConnectionDetails(), accumuloConn);
        deletePCJ.deletePCJ(getRyaInstanceName(), "randomID");
    }
}