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
package org.apache.rya.accumulo.mr.merge.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.accumulo.mr.merge.MergeTool;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.persist.RyaDAOException;
import twitter4j.Logger;

/**
 * Handles running a single {@link MiniAccumuloCluster} or a single {@link MockInstance} for an instance.
 */
public class AccumuloInstanceDriver {
    private static final Logger log = Logger.getLogger(AccumuloInstanceDriver.class);

    private static final boolean IS_COPY_HADOOP_HOME_ENABLED = true;

    public static final String ROOT_USER_NAME = "root";

    private final String driverName;
    private final boolean isMock;
    private final boolean shouldCreateIndices;
    private final boolean isReadOnly;
    private final boolean isParent;

    private final String user;
    private final String password;
    private final String instanceName;
    private final String tablePrefix;
    private final String auth;

    private Connector connector;

    private AccumuloRyaDAO dao;

    private SecurityOperations secOps;

    private final AccumuloRdfConfiguration config = new AccumuloRdfConfiguration();

    private MiniAccumuloCluster miniAccumuloCluster = null;

    private MockInstance mockInstance = null;

    private ZooKeeperInstance zooKeeperInstance = null;

    private Instance instance = null;

    private String zooKeepers;

    private final Map<String, String> configMap = new LinkedHashMap<>();

    private List<String> indices = null;

    private final List<String> tableList = new ArrayList<>();

    private File tempDir = null;

    public static final List<String> TABLE_NAME_SUFFIXES =
        ImmutableList.<String>of(
            RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX,
            RdfCloudTripleStoreConstants.TBL_PO_SUFFIX,
            RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX,
            RdfCloudTripleStoreConstants.TBL_NS_SUFFIX,
            RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX,
            RdfCloudTripleStoreConstants.TBL_STATS_SUFFIX,
            RdfCloudTripleStoreConstants.TBL_SEL_SUFFIX
        );

    /**
     * Creates a new instance of {@link AccumuloInstanceDriver}.
     * @param driverName the name used to identify this driver in the logs. (not {@code null})
     * @param isMock {@code true} if the instance will use {@link MockInstance}s.
     * {@code false} if the instance will use {@link MiniAccumuloCluster}s.
     * @param shouldCreateIndices {@code true} to create all the indices associated with a Rya deployment.
     * {@code false} otherwise.
     * @param isReadOnly {@code true} if all the tables in the instance should have their
     * table permissions set to read only.  {@code false} if the table permission are set to write.
     * @param isParent {@code true} if the instance is the parent/main instance. {@code false} if it's the
     * child.
     * @param user the user name tied to this instance.
     * @param password the password for the user.
     * @param instanceName the name of the instance.
     * @param tablePrefix the table prefix.
     * @param auth the comma-separated authorization list.
     */
    public AccumuloInstanceDriver(final String driverName, final boolean isMock, final boolean shouldCreateIndices, final boolean isReadOnly, final boolean isParent, final String user, final String password, final String instanceName, final String tablePrefix, final String auth) {
        this.driverName = Preconditions.checkNotNull(driverName);
        this.isMock = isMock;
        this.shouldCreateIndices = shouldCreateIndices;
        this.isReadOnly = isReadOnly;
        this.user = user;
        this.password = password;
        this.instanceName = instanceName;
        this.tablePrefix = tablePrefix;
        this.auth = auth;
        this.isParent = isParent;

        config.setTablePrefix(tablePrefix);
    }

    /**
     * Sets up the {@link AccumuloInstanceDriver}.
     * @throws Exception
     */
    public void setUp() throws Exception {
        setUpInstance();
        setUpTables();
        setUpDao();
        setUpConfig();
    }

    /**
     * Sets up the {@link MiniAccumuloCluster} or the {@link MockInstance}.
     * @throws Exception
     */
    public void setUpInstance() throws Exception {
        if (!isMock) {
            log.info("Setting up " + driverName + " MiniAccumulo cluster...");
            // Create and Run MiniAccumulo Cluster
            tempDir = Files.createTempDir();
            tempDir.deleteOnExit();
            miniAccumuloCluster = new MiniAccumuloCluster(tempDir, password);
            copyHadoopHomeToTemp();
            miniAccumuloCluster.getConfig().setInstanceName(instanceName);
            log.info(driverName + " MiniAccumulo instance starting up...");
            miniAccumuloCluster.start();
            Thread.sleep(1000);
            log.info(driverName + " MiniAccumulo instance started");
            log.info("Creating connector to " + driverName + " MiniAccumulo instance...");
            zooKeeperInstance = new ZooKeeperInstance(miniAccumuloCluster.getClientConfig());
            instance = zooKeeperInstance;
            connector = zooKeeperInstance.getConnector(user, new PasswordToken(password));
            log.info("Created connector to " + driverName + " MiniAccumulo instance");
        } else {
            log.info("Setting up " + driverName + " mock instance...");
            mockInstance = new MockInstance(instanceName);
            instance = mockInstance;
            connector = mockInstance.getConnector(user, new PasswordToken(password));
            log.info("Created connector to " + driverName + " mock instance");
        }
        zooKeepers = instance.getZooKeepers();
    }

    /**
     * Copies the HADOOP_HOME bin directory to the {@link MiniAccumuloCluster} temp directory.
     * {@link MiniAccumuloCluster} expects to find bin/winutils.exe in the MAC temp
     * directory instead of HADOOP_HOME for some reason.
     * @throws IOException
     */
    private void copyHadoopHomeToTemp() throws IOException {
        if (IS_COPY_HADOOP_HOME_ENABLED && SystemUtils.IS_OS_WINDOWS) {
            final String hadoopHomeEnv = System.getenv("HADOOP_HOME");
            if (hadoopHomeEnv != null) {
                final File hadoopHomeDir = new File(hadoopHomeEnv);
                if (hadoopHomeDir.exists()) {
                    final File binDir = Paths.get(hadoopHomeDir.getAbsolutePath(), "/bin").toFile();
                    if (binDir.exists()) {
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
     * Sets up all the tables and indices.
     * @throws Exception
     */
    public void setUpTables() throws Exception {
        // Setup tables and permissions
        log.info("Setting up " + driverName + " tables and permissions");
        for (final String tableSuffix : TABLE_NAME_SUFFIXES) {
            final String tableName = tablePrefix + tableSuffix;
            tableList.add(tableName);
            if (!connector.tableOperations().exists(tableName)) {
                connector.tableOperations().create(tableName);
            }
        }

        if (shouldCreateIndices) {
            indices = Arrays.asList(
                    /* TODO: SEE RYA-160
                ConfigUtils.getFreeTextDocTablename(config),
                ConfigUtils.getFreeTextTermTablename(config),
                ConfigUtils.getGeoTablename(config),
                ConfigUtils.getTemporalTableName(config),
                ConfigUtils.getEntityTableName(config)
                */
            );

            tableList.addAll(indices);

            log.info("Setting up " + driverName + " indices");
            for (final String index : indices) {
                if (!connector.tableOperations().exists(index)) {
                    connector.tableOperations().create(index);
                }
            }
        }

        // Setup user with authorizations
        log.info("Creating " + driverName + " user and authorizations");
        secOps = connector.securityOperations();
        if (!user.equals(ROOT_USER_NAME)) {
            secOps.createLocalUser(user, new PasswordToken(password));
        }
        addAuths(auth);
        final TablePermission tablePermission = isReadOnly ? TablePermission.READ : TablePermission.WRITE;
        for (final String tableSuffix : TABLE_NAME_SUFFIXES) {
            secOps.grantTablePermission(user, tablePrefix + tableSuffix, tablePermission);
        }
        if (shouldCreateIndices) {
            for (final String index : indices) {
                secOps.grantTablePermission(user, index, tablePermission);
            }
        }
    }

    /**
     * Sets up the {@link AccumuloRyaDAO}.
     * @throws Exception
     */
    public void setUpDao() throws Exception {
        // Setup dao
        log.info("Creating " + driverName + " DAO");
        dao = new AccumuloRyaDAO();
        dao.setConnector(connector);
        dao.setConf(config);

        // Flush the tables before initializing the DAO
        for (final String tableName : tableList) {
            connector.tableOperations().flush(tableName, null, null, false);
        }

        dao.init();
    }

    /**
     * Sets up the configuration and prints the arguments.
     */
    public void setUpConfig() {
        log.info("Setting " + driverName + " config");

        // Setup config
        if (isMock) {
            configMap.put(MRUtils.AC_MOCK_PROP, Boolean.TRUE.toString());
        }
        configMap.put(MRUtils.AC_INSTANCE_PROP, instanceName);
        configMap.put(MRUtils.AC_USERNAME_PROP, user);
        configMap.put(MRUtils.AC_PWD_PROP, password);
        configMap.put(MRUtils.TABLE_PREFIX_PROPERTY, tablePrefix);
        configMap.put(MRUtils.AC_AUTH_PROP, auth);
        configMap.put(MRUtils.AC_ZK_PROP, zooKeepers != null ? zooKeepers : "localhost");

        log.info(driverName + " config properties");
        config.setTablePrefix(tablePrefix);
        for (final Entry<String, String> entry : configMap.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            final String argument = ToolConfigUtils.makeArgument(isParent ? key : key + MergeTool.CHILD_SUFFIX, value);
            log.info(argument);
            config.set(key, value);
        }

        MergeTool.setDuplicateKeys(config);
    }

    /**
     * Tears down all the tables and indices.
     * @throws Exception
     */
    public void tearDownTables() throws Exception {
        // delete all tables.
        if (connector != null) {
            for (final String tableName : tableList) {
                if (connector.tableOperations().exists(tableName)) {
                    connector.tableOperations().delete(tableName);
                }
            }
        }
    }

    /**
     * Tears down the {@link AccumuloRyaDAO}.
     * @throws Exception
     */
    public void tearDownDao() throws Exception {
        if (dao != null) {
            log.info("Stopping " + driverName + " DAO");
            try {
                dao.destroy();
            } catch (final RyaDAOException e) {
                log.error("Error stopping " + driverName + " DAO", e);
            }
            dao = null;
        }
    }

    /**
     * Tears down the instance.
     * @throws Exception
     */
    public void tearDownInstance() throws Exception {
        if (miniAccumuloCluster != null) {
            log.info("Stopping " + driverName + " cluster");
            try {
                miniAccumuloCluster.stop();
            } catch (IOException | InterruptedException e) {
                log.error("Error stopping " + driverName + " cluster", e);
            }
            miniAccumuloCluster = null;
        }
    }

    /**
     * Tears down the {@link AccumuloInstanceDriver}.
     * @throws Exception
     */
    public void tearDown() throws Exception {
        try {
            //tearDownTables();
            tearDownDao();
            tearDownInstance();
        } finally {
            removeTempDir();
        }
    }

    /**
     * Deletes the {@link MiniAccumuloCluster} temporary directory.
     */
    public void removeTempDir() {
        if (tempDir != null) {
            try {
                FileUtils.deleteDirectory(tempDir);
            } catch (final IOException e) {
                log.error("Error deleting " + driverName + " temp directory", e);
            }
            tempDir = null;
        }
    }

    /**
     * Adds authorizations to the {@link SecurityOperations} of this instance's user.
     * @param auths the list of authorizations to add.
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    public void addAuths(final String... auths) throws AccumuloException, AccumuloSecurityException {
        final Authorizations newAuths = AccumuloRyaUtils.addUserAuths(user, secOps, auths);
        secOps.changeUserAuthorizations(user, newAuths);
    }

    /**
     * @return the {@link Authorizations} of this instance's user.
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    public Authorizations getAuths() throws AccumuloException, AccumuloSecurityException {
        if (secOps != null) {
            return secOps.getUserAuthorizations(user);
        } else {
            return null;
        }
    }

    /**
     * @return {@code true} if this is a mock instance.  {@code false} if this is a MiniAccumuloCluster instance.
     */
    public boolean isMock() {
        return isMock;
    }

    /**
     * @return {@code true} to create all the indices associated with a Rya deployment.
     * {@code false} otherwise.
     */
    public boolean shouldCreateIndices() {
        return shouldCreateIndices;
    }

    /**
     * @return {@code true} if all the tables in the instance should have their
     * table permissions set to read only.  {@code false} if the table permission are set to write.
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * @return the user name tied to this instance
     */
    public String getUser() {
        return user;
    }

    /**
     * @return the password for the user.
     */
    public String getPassword() {
        return password;
    }

    /**
     * @return the name of the instance.
     */
    public String getInstanceName() {
        return instanceName;
    }

    /**
     * @return the table prefix.
     */
    public String getTablePrefix() {
        return tablePrefix;
    }

    /**
     * @return the comma-separated authorization list.
     */
    public String getAuth() {
        return auth;
    }

    /**
     * @return the {@link Connector} to this instance.
     */
    public Connector getConnector() {
        return connector;
    }

    /**
     * Sets the {@link Connector} to this instance.
     * @param connector the {@link Connector}.
     */
    public void setConnector(final Connector connector) {
        this.connector = connector;
    }

    /**
     * @return the {@link AccumuloRyaDAO}.
     */
    public AccumuloRyaDAO getDao() {
        return dao;
    }

    /**
     * @return the {@link SecurityOperations}.
     */
    public SecurityOperations getSecOps() {
        return secOps;
    }

    /**
     * @return the {@link AccumuloRdfConfiguration}.
     */
    public AccumuloRdfConfiguration getConfig() {
        return config;
    }

    /**
     * @return the {@link MiniAccumuloCluster} for this instance or {@code null} if
     * this is a {@link MockInstance}.
     */
    public MiniAccumuloCluster getMiniAccumuloCluster() {
        return miniAccumuloCluster;
    }

    /**
     * @return the {@link MockInstance} for this instance or {@code null} if
     * this is a {@link MiniAccumuloCluster}.
     */
    public MockInstance getMockInstance() {
        return mockInstance;
    }

    /**
     * @return the {@link ZooKeeperInstance} for this instance or {@code null} if
     * this is a {@link MockInstance}.
     */
    public ZooKeeperInstance getZooKeeperInstance() {
        return zooKeeperInstance;
    }

    /**
     * @return the {@link ZooKeepInstance} or {@link MockInstance}.
     */
    public Instance getInstance() {
        return instance;
    }

    /**
     * @return the comma-separated list of zoo keeper host names.
     */
    public String getZooKeepers() {
        return zooKeepers;
    }

    /**
     * @return an unmodifiable map of the configuration keys and values.
     */
    public Map<String, String> getConfigMap() {
        return Collections.unmodifiableMap(configMap);
    }

    /**
     * @return an unmodifiable list of the table names and indices.
     */
    public List<String> getTableList() {
        return Collections.unmodifiableList(tableList);
    }

    /**
     * @return the {@link MiniAccumuloCluster} temporary directory for this instance or {@code null}
     * if it's a {@link MockInstance}.
     */
    public File getTempDir() {
        return tempDir;
    }
}