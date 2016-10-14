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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ClientCnxn;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Boilerplate code for a unit test that uses a {@link MiniAccumuloCluster}.
 * <p>
 * It uses the same instance of {@link MiniAccumuloCluster} and just clears out
 * any tables that were added between tests.
 */
public class AccumuloITBase {

    // Managed the MiniAccumuloCluster
    private MiniAccumuloClusterInstance cluster = null;

    @BeforeClass
    public static void killLoudLogs() {
        Logger.getLogger(ClientCnxn.class).setLevel(Level.ERROR);
    }

    @Before
    public void initCluster() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        cluster = new MiniAccumuloClusterInstance();
        cluster.startMiniAccumulo();
    }


    @After
    public void tearDownCluster() throws IOException, InterruptedException {
        cluster.stopMiniAccumulo();
    }

    /**
     * @return The {@link MiniAccumuloClusterInstance} used by the tests.
     */
    public MiniAccumuloClusterInstance getClusterInstance() {
        return cluster;
    }

    /**
     * @return The root username.
     */
    public String getUsername() {
        return cluster.getUsername();
    }

    /**
     * @return The root password.
     */
    public String getPassword() {
        return cluster.getPassword();
    }

    /**
     * @return The MiniAccumulo's zookeeper instance name.
     */
    public String getInstanceName() {
        return cluster.getInstanceName();
    }

    /**
     * @return The MiniAccumulo's zookeepers.
     */
    public String getZookeepers() {
        return cluster.getZookeepers();
    }

    /**
     * @return A {@link Connector} that creates connections to the mini accumulo cluster.
     * @throws AccumuloException Could not connect to the cluster.
     * @throws AccumuloSecurityException Could not connect to the cluster because of a security violation.
     */
    public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
        return cluster.getConnector();
    }
}