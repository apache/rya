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
package org.apache.rya.export.accumulo.driver;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.Logger;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;

/**
 * Handles running a {@link MiniAccumuloCluster} or a {@link MockInstance} for a parent and child instance for testing.
 */
public class AccumuloDualInstanceDriver {
    private static final Logger log = Logger.getLogger(AccumuloDualInstanceDriver.class);

    private final InstanceType instanceType;
    private final boolean isMock;
    private final boolean shouldCreateIndices;
    private final boolean isParentReadOnly;
    private final boolean isChildReadOnly;
    private final boolean doesChildInitiallyExist;

    public static final String PARENT_USER_NAME = "parent_user";
    public static final String PARENT_PASSWORD = "parent_pwd";
    public static final String PARENT_INSTANCE = "parent_instance";
    public static final String PARENT_TABLE_PREFIX = "pt_";
    public static final String PARENT_AUTH = "parent_auth";
    public static final String PARENT_ZOOKEEPERS = "localhost:1111";
    public static final ColumnVisibility PARENT_COLUMN_VISIBILITY = new ColumnVisibility(PARENT_AUTH);

    public static final String CHILD_USER_NAME = "child_user";
    public static final String CHILD_PASSWORD = "child_pwd";
    public static final String CHILD_INSTANCE = "child_instance";
    public static final String CHILD_TABLE_PREFIX = "ct_";
    public static final String CHILD_AUTH = "child_auth";
    public static final String CHILD_ZOOKEEPERS = "localhost:2222";
    public static final ColumnVisibility CHILD_COLUMN_VISIBILITY = new ColumnVisibility(CHILD_AUTH);

    private final AccumuloInstanceDriver parentAccumuloInstanceDriver;
    private final AccumuloInstanceDriver childAccumuloInstanceDriver;

    /**
     * Creates a new instance of {@link AccumuloDualInstanceDriver}.
     * @param instanceType the instanceType of this driver.
     * @param shouldCreateIndices {@code true} to create all the indices associated with a Rya deployment.
     * {@code false} otherwise.
     * @param isParentReadOnly {@code true} if all the tables in the parent instance should have their
     * table permissions set to read only.  {@code false} if the table permission are set to write.
     * @param isChildReadOnly {@code true} if all the tables in the child instance should have their
     * table permissions set to read only.  {@code false} if the table permission are set to write.
     * @param doesChildInitiallyExist {@code true} if all the child instance exists initially.
     * {@code false} otherwise.
     */
    public AccumuloDualInstanceDriver(final InstanceType instanceType, final boolean shouldCreateIndices, final boolean isParentReadOnly, final boolean isChildReadOnly, final boolean doesChildInitiallyExist) {
        this.instanceType = instanceType;
        this.isMock = instanceType.isMock();
        this.shouldCreateIndices = shouldCreateIndices;
        this.isParentReadOnly = isParentReadOnly;
        this.isChildReadOnly = isChildReadOnly;
        this.doesChildInitiallyExist = doesChildInitiallyExist;
        final String parentUser =  isMock ? PARENT_USER_NAME : AccumuloInstanceDriver.ROOT_USER_NAME;
        final String childUser = isMock ? CHILD_USER_NAME : AccumuloInstanceDriver.ROOT_USER_NAME;
        parentAccumuloInstanceDriver = new AccumuloInstanceDriver("Parent", instanceType, shouldCreateIndices, isParentReadOnly, true, parentUser, PARENT_PASSWORD, PARENT_INSTANCE, PARENT_TABLE_PREFIX, PARENT_AUTH, PARENT_ZOOKEEPERS);
        childAccumuloInstanceDriver = new AccumuloInstanceDriver("Child", instanceType, shouldCreateIndices, isChildReadOnly, false, childUser, CHILD_PASSWORD, CHILD_INSTANCE, CHILD_TABLE_PREFIX, CHILD_AUTH, CHILD_ZOOKEEPERS);
    }

    /**
     * Sets up the parent and child {@link AccumuloInstanceDriver}s.
     * @throws Exception
     */
    public void setUp() throws Exception {
        log.info("Setting up parent and child drivers.");
        setUpInstances();
        setUpTables();
        setUpDaos();
        setUpConfigs();
    }

    /**
     * Sets up the parent and child instances.
     * @throws Exception
     */
    public void setUpInstances() throws Exception {
        parentAccumuloInstanceDriver.setUpInstance();
        if (doesChildInitiallyExist) {
            childAccumuloInstanceDriver.setUpInstance();
        }
    }

    /**
     * Sets up all the tables and indices for the parent and child instances.
     * @throws Exception
     */
    public void setUpTables() throws Exception {
        parentAccumuloInstanceDriver.setUpTables();
        if (doesChildInitiallyExist) {
            childAccumuloInstanceDriver.setUpTables();
        }
    }

    /**
     * Sets up the {@link AccumuloRyaDAO}s for the parent and child instances.
     * @throws Exception
     */
    public void setUpDaos() throws Exception {
        parentAccumuloInstanceDriver.setUpDao();
        if (doesChildInitiallyExist) {
            childAccumuloInstanceDriver.setUpDao();
        }
    }

    /**
     * Sets up the configuration and prints the arguments for the parent and child instances.
     */
    public void setUpConfigs() {
        parentAccumuloInstanceDriver.setUpConfig();
        childAccumuloInstanceDriver.setUpConfig();
    }

    /**
     * Tears down all the tables and indices for the parent and child instances.
     * @throws Exception
     */
    public void tearDownTables() throws Exception {
        parentAccumuloInstanceDriver.tearDownTables();
        childAccumuloInstanceDriver.tearDownTables();
    }

    /**
     * Tears down the {@link AccumuloRyaDAO}s for the parent and child instances.
     * @throws Exception
     */
    public void tearDownDaos() throws Exception {
        parentAccumuloInstanceDriver.tearDownDao();
        childAccumuloInstanceDriver.tearDownDao();
    }

    /**
     * Tears down the parent and child instances.
     * @throws Exception
     */
    public void tearDownInstances() throws Exception {
        parentAccumuloInstanceDriver.tearDownInstance();
        childAccumuloInstanceDriver.tearDownInstance();
    }

    /**
     * Tears down the {@link AccumuloInstanceDriver} for the parent and child instances.
     * @throws Exception
     */
    public void tearDown() throws Exception {
        try {
            //tearDownTables();
            tearDownDaos();
            tearDownInstances();
        } finally {
            removeTempDirs();
        }
    }

    /**
     * Deletes the {@link MiniAccumuloCluster} temporary directories for the parent and child instances.
     */
    private void removeTempDirs() {
        parentAccumuloInstanceDriver.removeTempDir();
        childAccumuloInstanceDriver.removeTempDir();
    }

    /**
     * Adds authorizations to the {@link SecurityOperations} of the parent instance's user.
     * @param auths the list of authorizations to add.
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    public void addParentAuths(final String... auths) throws AccumuloException, AccumuloSecurityException {
        parentAccumuloInstanceDriver.addAuths(auths);
    }

    /**
     * Adds authorizations to the {@link SecurityOperations} of the child instance's user.
     * @param auths the list of authorizations to add.
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    public void addChildAuths(final String... auths) throws AccumuloException, AccumuloSecurityException {
        childAccumuloInstanceDriver.addAuths(auths);
    }

    /**
     * @return the {@link Authorizations} of the parent instance's user.
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    public Authorizations getParentAuths() throws AccumuloException, AccumuloSecurityException {
        return parentAccumuloInstanceDriver.getAuths();
    }

    /**
     * @return the {@link Authorizations} of the child instance's user.
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    public Authorizations getChildAuths() throws AccumuloException, AccumuloSecurityException {
        return childAccumuloInstanceDriver.getAuths();
    }

    /**
     * Adds a {@link Collection} of {@link RyaStatement}s to the parent instance's DAO.
     * @param ryaStatements the {@link Collection} of {@link RyaStatement}s.
     * @throws RyaDAOException
     */
    public void addParentRyaStatements(final Collection<RyaStatement> ryaStatements) throws RyaDAOException {
        addRyaStatements(ryaStatements.iterator(), parentAccumuloInstanceDriver.getDao());
    }

    /**
     * Adds a {@link Collection} of {@link RyaStatement}s to the child instance's DAO.
     * @param ryaStatements the {@link Collection} of {@link RyaStatement}s.
     * @throws RyaDAOException
     */
    public void addChildRyaStatements(final Collection<RyaStatement> ryaStatements) throws RyaDAOException {
        addRyaStatements(ryaStatements.iterator(), childAccumuloInstanceDriver.getDao());
    }

    /**
     * Adds {@link RyaStatement}s to the parent instance's DAO from the provided {@link Iterator}.
     * @param ryaStatementIterator the {@link RyaStatement} {@link Iterator}.
     * @throws RyaDAOException
     */
    public void addParentRyaStatements(final Iterator<RyaStatement> ryaStatementIterator) throws RyaDAOException {
        addRyaStatements(ryaStatementIterator, parentAccumuloInstanceDriver.getDao());
    }

    /**
     * Adds {@link RyaStatement}s to the child instance's DAO from the provided {@link Iterator}.
     * @param ryaStatementIterator the {@link RyaStatement} {@link Iterator}.
     * @throws RyaDAOException
     */
    public void addChildRyaStatements(final Iterator<RyaStatement> ryaStatementIterator) throws RyaDAOException {
        addRyaStatements(ryaStatementIterator, childAccumuloInstanceDriver.getDao());
    }

    /**
     * Adds a {@link RyaStatement} to the parent instance's DAO.
     * @param ryaStatement the {@link RyaStatement}.
     * @throws RyaDAOException
     */
    public void addParentRyaStatement(final RyaStatement ryaStatement) throws RyaDAOException {
        addRyaStatement(ryaStatement, parentAccumuloInstanceDriver.getDao());
    }

    /**
     * Adds a {@link RyaStatement} to the child instance's DAO.
     * @param ryaStatement the {@link RyaStatement}.
     * @throws RyaDAOException
     */
    public void addChildRyaStatement(final RyaStatement ryaStatement) throws RyaDAOException {
        addRyaStatement(ryaStatement, childAccumuloInstanceDriver.getDao());
    }

    /**
     * Adds {@link RyaStatement}s to specified DAO from the provided {@link Iterator}.
     * @param ryaStatementIterator the {@link RyaStatement} {@link Iterator}.
     * @param dao the {@link AccumuloRyaDAO}.
     * @throws RyaDAOException
     */
    private static void addRyaStatements(final Iterator<RyaStatement> ryaStatementIterator, final AccumuloRyaDAO dao) throws RyaDAOException {
        dao.add(ryaStatementIterator);
    }

    /**
     * Adds a {@link RyaStatement} to the specified DAO.
     * @param ryaStatement the {@link RyaStatement}.
     * @throws RyaDAOException
     */
    private static void addRyaStatement(final RyaStatement ryaStatement, final AccumuloRyaDAO dao) throws RyaDAOException {
        dao.add(ryaStatement);
    }

    /**
     * @return the parent instance's {@link AccumuloInstanceDriver}.
     */
    public AccumuloInstanceDriver getParentAccumuloInstanceDriver() {
        return parentAccumuloInstanceDriver;
    }

    /**
     * @return the child instance's {@link AccumuloInstanceDriver}.
     */
    public AccumuloInstanceDriver getChildAccumuloInstanceDriver() {
        return childAccumuloInstanceDriver;
    }

    /**
     * @return the {@link InstanceType} of this driver.
     */
    public InstanceType getInstanceType() {
        return instanceType;
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
     * @return {@code true} if all the tables in the parent instance should have their
     * table permissions set to read only.  {@code false} if the table permission are set to write.
     */
    public boolean isParentReadOnly() {
        return isParentReadOnly;
    }

    /**
     * @return {@code true} if all the tables in the child instance should have their
     * table permissions set to read only.  {@code false} if the table permission are set to write.
     */
    public boolean isChildReadOnly() {
        return isChildReadOnly;
    }

    /**
     * @return {@code true} if all the child instance exists initially.
     * {@code false} otherwise.
     */
    public boolean doesChildInitiallyExist() {
        return doesChildInitiallyExist;
    }

    /**
     * @return the user name tied to the parent instance.
     */
    public String getParentUser() {
        return parentAccumuloInstanceDriver.getUser();
    }

    /**
     * @return the user name tied to the child instance.
     */
    public String getChildUser() {
        return childAccumuloInstanceDriver.getUser();
    }

    /**
     * @return the password for the parent instance's user.
     */
    public String getParentPassword() {
        return parentAccumuloInstanceDriver.getPassword();
    }

    /**
     * @return the password for the child instance's user.
     */
    public String getChildPassword() {
        return childAccumuloInstanceDriver.getPassword();
    }

    /**
     * @return the name of the parent instance.
     */
    public String getParentInstanceName() {
        return parentAccumuloInstanceDriver.getInstanceName();
    }

    /**
     * @return the name of the child instance.
     */
    public String getChildInstanceName() {
        return childAccumuloInstanceDriver.getInstanceName();
    }

    /**
     * @return the parent instance's table prefix.
     */
    public String getParentTablePrefix() {
        return parentAccumuloInstanceDriver.getTablePrefix();
    }

    /**
     * @return the child instance's table prefix.
     */
    public String getChildTablePrefix() {
        return childAccumuloInstanceDriver.getTablePrefix();
    }

    /**
     * @return the comma-separated authorization list for the parent instance.
     */
    public String getParentAuth() {
        return parentAccumuloInstanceDriver.getAuth();
    }

    /**
     * @return the comma-separated authorization list for the child instance.
     */
    public String getChildAuth() {
        return childAccumuloInstanceDriver.getAuth();
    }

    /**
     * @return the {@link Connector} to the parent instance.
     */
    public Connector getParentConnector() {
        return parentAccumuloInstanceDriver.getConnector();
    }

    /**
     * @return the {@link Connector} to the child instance.
     */
    public Connector getChildConnector() {
        return childAccumuloInstanceDriver.getConnector();
    }

    /**
     * @return the {@link AccumuloRyaDAO} for the parent instance.
     */
    public AccumuloRyaDAO getParentDao() {
        return parentAccumuloInstanceDriver.getDao();
    }

    /**
     * @return the {@link AccumuloRyaDAO} for the child instance.
     */
    public AccumuloRyaDAO getChildDao() {
        return childAccumuloInstanceDriver.getDao();
    }

    /**
     * @return the {@link SecurityOperations} for the parent instance.
     */
    public SecurityOperations getParentSecOps() {
        return parentAccumuloInstanceDriver.getSecOps();
    }

    /**
     * @return the {@link SecurityOperations} for the child instance.
     */
    public SecurityOperations getChildSecOps() {
        return childAccumuloInstanceDriver.getSecOps();
    }

    /**
     * @return the {@link AccumuloRdfConfiguration} for the parent instance.
     */
    public AccumuloRdfConfiguration getParentConfig() {
        return parentAccumuloInstanceDriver.getConfig();
    }

    /**
     * @return the {@link AccumuloRdfConfiguration} for the child instance.
     */
    public AccumuloRdfConfiguration getChildConfig() {
        return childAccumuloInstanceDriver.getConfig();
    }

    /**
     * @return the {@link MiniAccumuloCluster} for the parent instance or {@code null}
     * if this is a {@link MockInstance}.
     */
    public MiniAccumuloCluster getParentMiniAccumuloCluster() {
        return parentAccumuloInstanceDriver.getMiniAccumuloCluster();
    }

    /**
     * @return the {@link MiniAccumuloCluster} for the child instance or {@code null}
     * if this is a {@link MockInstance}.
     */
    public MiniAccumuloCluster getChildMiniAccumuloCluster() {
        return childAccumuloInstanceDriver.getMiniAccumuloCluster();
    }

    /**
     * @return the {@link MockInstance} for the parent instance or {@code null}
     * if this is a {@link MiniAccumuloCluster}.
     */
    public MockInstance getParentMockInstance() {
        return parentAccumuloInstanceDriver.getMockInstance();
    }

    /**
     * @return the {@link MockInstance} for the child instance or {@code null}
     * if this is a {@link MiniAccumuloCluster}.
     */
    public MockInstance getChildMockInstance() {
        return childAccumuloInstanceDriver.getMockInstance();
    }

    /**
     * @return the {@link ZooKeeperInstance} for the parent instance or {@code null} if
     * this is a {@link MockInstance}.
     */
    public ZooKeeperInstance getParentZooKeeperInstance() {
        return parentAccumuloInstanceDriver.getZooKeeperInstance();
    }

    /**
     * @return the {@link ZooKeeperInstance} for the child instance or {@code null} if
     * this is a {@link MockInstance}.
     */
    public ZooKeeperInstance getChildZooKeeperInstance() {
        return childAccumuloInstanceDriver.getZooKeeperInstance();
    }

    /**
     * @return the parent {@link ZooKeepInstance} or {@link MockInstance}.
     */
    public Instance getParentInstance() {
        return parentAccumuloInstanceDriver.getInstance();
    }

    /**
     * @return the child {@link ZooKeepInstance} or {@link MockInstance}.
     */
    public Instance getChildInstance() {
        return childAccumuloInstanceDriver.getInstance();
    }

    /**
     * @return the comma-separated list of zoo keeper host names for the parent instance.
     */
    public String getParentZooKeepers() {
        return parentAccumuloInstanceDriver.getZooKeepers();
    }

    /**
     * @return the comma-separated list of zoo keeper host names for the child instance.
     */
    public String getChildZooKeepers() {
        return childAccumuloInstanceDriver.getZooKeepers();
    }

    /**
     * @return an unmodifiable map of the configuration keys and values for the parent instance.
     */
    public Map<String, String> getParentConfigMap() {
        return parentAccumuloInstanceDriver.getConfigMap();
    }

    /**
     * @return an unmodifiable map of the configuration keys and values for the child instance.
     */
    public Map<String, String> getChildConfigMap() {
        return childAccumuloInstanceDriver.getConfigMap();
    }

    /**
     * @return an unmodifiable list of the table names and indices for the parent instance.
     */
    public List<String> getParentTableList() {
        return parentAccumuloInstanceDriver.getTableList();
    }

    /**
     * @return an unmodifiable list of the table names and indices for the child instance.
     */
    public List<String> getChildTableList() {
        return childAccumuloInstanceDriver.getTableList();
    }

    /**
     * @return the {@link MiniAccumuloCluster} temporary directory for the parent instance or {@code null}
     * if it's a {@link MockInstance}.
     */
    public File getParentTempDir() {
        return parentAccumuloInstanceDriver.getTempDir();
    }

    /**
     * @return the {@link MiniAccumuloCluster} temporary directory for the child instance or {@code null}
     * if it's a {@link MockInstance}.
     */
    public File getChildTempDir() {
        return childAccumuloInstanceDriver.getTempDir();
    }
}