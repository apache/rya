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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.apache.rya.indexing.pcj.fluo.api.ListQueryIds;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryException;

import com.google.common.collect.Sets;

import org.apache.rya.api.client.CreatePCJ;
import org.apache.rya.api.client.DeletePCJ;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;

/**
 * Integration tests the methods of {@link AccumuloCreatePCJ}.
 */
public class AccumuloDeletePCJIT extends FluoITBase {

    @Test
    public void deletePCJ() throws InstanceDoesNotExistException, RyaClientException, PCJStorageException, RepositoryException {
        // Initialize the commands that will be used by this test.
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                ACCUMULO_USER,
                ACCUMULO_PASSWORD.toCharArray(),
                super.cluster.getInstanceName(),
                super.cluster.getZooKeepers());

        final CreatePCJ createPCJ = new AccumuloCreatePCJ(connectionDetails, accumuloConn);

        // Create a PCJ.
        final String sparql =
                "SELECT ?x " +
                  "WHERE { " +
                  "?x <http://talksTo> <http://Eve>. " +
                  "?x <http://worksAt> <http://TacoJoint>." +
                "}";
        final String pcjId = createPCJ.createPCJ(RYA_INSTANCE_NAME, sparql);

        // Verify a Query ID was added for the query within the Fluo app.
        List<String> fluoQueryIds = new ListQueryIds().listQueryIds(fluoClient);
        assertEquals(1, fluoQueryIds.size());

        // Insert some statements into Rya.
        final ValueFactory vf = ryaRepo.getValueFactory();
        ryaConn.add(vf.createURI("http://Alice"), vf.createURI("http://talksTo"), vf.createURI("http://Eve"));
        ryaConn.add(vf.createURI("http://Bob"), vf.createURI("http://talksTo"), vf.createURI("http://Eve"));
        ryaConn.add(vf.createURI("http://Charlie"), vf.createURI("http://talksTo"), vf.createURI("http://Eve"));

        ryaConn.add(vf.createURI("http://Eve"), vf.createURI("http://helps"), vf.createURI("http://Kevin"));

        ryaConn.add(vf.createURI("http://Bob"), vf.createURI("http://worksAt"), vf.createURI("http://TacoJoint"));
        ryaConn.add(vf.createURI("http://Charlie"), vf.createURI("http://worksAt"), vf.createURI("http://TacoJoint"));
        ryaConn.add(vf.createURI("http://Eve"), vf.createURI("http://worksAt"), vf.createURI("http://TacoJoint"));
        ryaConn.add(vf.createURI("http://David"), vf.createURI("http://worksAt"), vf.createURI("http://TacoJoint"));

        // Verify the correct results were exported.
        fluo.waitForObservers();

        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final Set<BindingSet> results = Sets.newHashSet( pcjStorage.listResults(pcjId) );

        final MapBindingSet bob = new MapBindingSet();
        bob.addBinding("x", vf.createURI("http://Bob"));

        final MapBindingSet charlie = new MapBindingSet();
        charlie.addBinding("x", vf.createURI("http://Charlie"));

        final Set<BindingSet> expected = Sets.<BindingSet>newHashSet(bob, charlie);
        assertEquals(expected, results);

        // Delete the PCJ.
        final DeletePCJ deletePCJ = new AccumuloDeletePCJ(connectionDetails, accumuloConn);
        deletePCJ.deletePCJ(RYA_INSTANCE_NAME, pcjId);

        // Ensure the PCJ's metadata has been removed from the storage.
        assertTrue( pcjStorage.listPcjs().isEmpty() );

        // Ensure the PCJ has been removed from the Fluo application.
        fluo.waitForObservers();

        // Verify a Query ID was added for the query within the Fluo app.
        fluoQueryIds = new ListQueryIds().listQueryIds(fluoClient);
        assertEquals(0, fluoQueryIds.size());
    }

    @Test(expected = InstanceDoesNotExistException.class)
    public void deletePCJ_instanceDoesNotExist() throws InstanceDoesNotExistException, RyaClientException {
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                ACCUMULO_USER,
                ACCUMULO_PASSWORD.toCharArray(),
                super.cluster.getInstanceName(),
                super.cluster.getZooKeepers());

        // Delete a PCJ for a Rya instance that doesn't exist.
        final DeletePCJ deletePCJ = new AccumuloDeletePCJ(connectionDetails, accumuloConn);
        deletePCJ.deletePCJ("doesNotExist", "randomID");
    }

    @Test(expected = RyaClientException.class)
    public void deletePCJ_pcjDoesNotExist() throws InstanceDoesNotExistException, RyaClientException {
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                ACCUMULO_USER,
                ACCUMULO_PASSWORD.toCharArray(),
                super.cluster.getInstanceName(),
                super.cluster.getZooKeepers());

        // Delete the PCJ.
        final DeletePCJ deletePCJ = new AccumuloDeletePCJ(connectionDetails, accumuloConn);
        deletePCJ.deletePCJ(RYA_INSTANCE_NAME, "randomID");
    }
}