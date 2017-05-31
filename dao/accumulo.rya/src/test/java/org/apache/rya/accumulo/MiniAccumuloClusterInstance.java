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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.log4j.Logger;

import com.google.common.io.Files;

/**
 * Contains boilerplate code that can be used by an integration test that
 * uses a {@link MiniAccumuloCluster}.
 * <p>
 * You can just extend {@link AccumuloITBase} if your test only requires Accumulo.
 */
public class MiniAccumuloClusterInstance {

    private static final Logger log = Logger.getLogger(MiniAccumuloClusterInstance.class);

    private static final String USERNAME = "root";
    private static final String PASSWORD = "password";

    /**
     * A mini Accumulo cluster that can be used by the tests.
     */
    private static MiniAccumuloCluster cluster = null;

    /**
     * Start the {@link MiniAccumuloCluster}.
     */
    public void startMiniAccumulo() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        final File miniDataDir = Files.createTempDir();

        // Setup and start the Mini Accumulo.
        final MiniAccumuloConfig cfg = new MiniAccumuloConfig(miniDataDir, PASSWORD);
        cluster = new MiniAccumuloCluster(cfg);
        copyHadoopHomeToTemp();
        cluster.start();
    }

    /**
     * Stop the {@link MiniAccumuloCluster}.
     */
    public void stopMiniAccumulo() throws IOException, InterruptedException {
        if(cluster != null) {
            try {
                log.info("Shutting down the Mini Accumulo being used as a Rya store.");
                cluster.stop();
                log.info("Mini Accumulo being used as a Rya store shut down.");
            } catch(final Exception e) {
                log.error("Could not shut down the Mini Accumulo.", e);
            }
        }
    }

    /**
     * Copies the HADOOP_HOME bin directory to the {@link MiniAccumuloCluster} temp directory.
     * {@link MiniAccumuloCluster} expects to find bin/winutils.exe in the MAC temp
     * directory instead of HADOOP_HOME for some reason.
     * @throws IOException
     */
    private static void copyHadoopHomeToTemp() throws IOException {
        if (SystemUtils.IS_OS_WINDOWS && cluster != null) {
            final String hadoopHomeEnv = System.getenv("HADOOP_HOME");
            if (hadoopHomeEnv != null) {
                final File hadoopHomeDir = new File(hadoopHomeEnv);
                if (hadoopHomeDir.exists()) {
                    final File binDir = Paths.get(hadoopHomeDir.getAbsolutePath(), "/bin").toFile();
                    if (binDir.exists()) {
                        final File tempDir = cluster.getConfig().getDir();
                        FileUtils.copyDirectoryToDirectory(binDir, tempDir);
                    } else {
                        log.warn("The specified path for the Hadoop bin directory does not exist: " + binDir.getAbsolutePath());
                    }
                } else {
                    log.warn("The specified path for HADOOP_HOME does not exist: " + hadoopHomeDir.getAbsolutePath());
                }
            } else {
                log.warn("The HADOOP_HOME environment variable was not found.");
            }
        }
    }

    /**
     * @return The {@link MiniAccumuloCluster} managed by this class.
     */
    public MiniAccumuloCluster getCluster() {
        return cluster;
    }

    /**
     * @return An Accumulo connector that is connected to the mini cluster's root account.
     */
    public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
        return cluster.getConnector(USERNAME, PASSWORD);
    }

    /**
     * @return The root username.
     */
    public String getUsername() {
        return USERNAME;
    }

    /**
     * @return The root password.
     */
    public String getPassword() {
        return PASSWORD;
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
        return cluster.getZooKeepers();
    }
}