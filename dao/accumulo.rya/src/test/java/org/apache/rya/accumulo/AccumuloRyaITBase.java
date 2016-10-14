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
package org.apache.rya.accumulo;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.google.common.base.Optional;

import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.GeoIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.AlreadyInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;

/**
 * Contains boilerplate code for spinning up a Mini Accumulo Cluster and initializing
 * some of the Rya stuff. We can not actually initialize an instance of Rya here
 * because Sail is not available to us.
 */
public class AccumuloRyaITBase {

    // Managed the MiniAccumuloCluster
    private static final MiniAccumuloClusterInstance cluster = new MiniAccumuloClusterInstance();

    // Manage the Rya instances that are hosted on the cluster
    protected static final AtomicInteger ryaInstanceNameCounter = new AtomicInteger(1);
    private String ryaInstanceName;

    @BeforeClass
    public static void initCluster() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        cluster.startMiniAccumulo();
    }

    @Before
    public void prepareForNextTest() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, AlreadyInitializedException, RyaDetailsRepositoryException {
        // Get the next Rya instance name.
        ryaInstanceName = "testInstance" + ryaInstanceNameCounter.getAndIncrement() + "_";

        // Create Rya Details for the instance name.
        final RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(cluster.getConnector(), ryaInstanceName);

        final RyaDetails details = RyaDetails.builder()
                .setRyaInstanceName(ryaInstanceName)
                .setRyaVersion("0.0.0.0")
                .setFreeTextDetails( new FreeTextIndexDetails(true) )
                .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
                .setGeoIndexDetails( new GeoIndexDetails(true) )
                .setTemporalIndexDetails( new TemporalIndexDetails(true) )
                .setPCJIndexDetails(
                        PCJIndexDetails.builder()
                            .setEnabled(true) )
                .setJoinSelectivityDetails( new JoinSelectivityDetails( Optional.<Date>absent() ) )
                .setProspectorDetails( new ProspectorDetails( Optional.<Date>absent() ))
                .build();

        detailsRepo.initialize(details);
    }

    @AfterClass
    public static void tearDownCluster() throws IOException, InterruptedException {
        cluster.stopMiniAccumulo();
    }

    /**
     * @return The {@link MiniAccumuloClusterInstance} used by the tests.
     */
    public MiniAccumuloClusterInstance getClusterInstance() {
        return cluster;
    }

    /**
     * @return The name of the Rya instance that is being used for the current test.
     */
    public String getRyaInstanceName() {
        return ryaInstanceName;
    }
}