package org.apache.rya.api.persist;

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



import java.util.Iterator;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.query.RyaQueryEngine;

/**
 * Provides the access layer to the Rya triple store.
 *
 * Date: Feb 28, 2012
 * Time: 3:30:14 PM
 */
public interface RyaDAO<C extends RdfCloudTripleStoreConfiguration> extends RyaConfigured<C> {

    /**
     * Initialize the RyaDAO. Should only be called once, otherwise, if already initialized, it will
     * throw an exception.
     *
     * @throws RyaDAOException
     */
    public void init() throws RyaDAOException;

    /**
     *
     * @return true if the store is already initiailized
     * @throws RyaDAOException
     */
    public boolean isInitialized() throws RyaDAOException;

    /**
     * Shutdown the store. To reinitialize, call the init() method.
     *
     * @throws RyaDAOException
     */
    public void destroy() throws RyaDAOException;

    /**
     * Add and commit a single RyaStatement
     *
     * @param statement
     * @throws RyaDAOException
     */
    public void add(RyaStatement statement) throws RyaDAOException;

    /**
     * Add and commit a collection of RyaStatements
     *
     * @param statement
     * @throws RyaDAOException
     */
    public void add(Iterator<RyaStatement> statement) throws RyaDAOException;

    /**
     * Delete a RyaStatement. The Configuration should provide the auths to perform the delete
     *
     * @param statement
     * @param conf
     * @throws RyaDAOException
     */
    public void delete(RyaStatement statement, C conf) throws RyaDAOException;

    /**
     * Drop a set of Graphs. The Configuration should provide the auths to perform the delete
     *
     * @param conf
     * @throws RyaDAOException
     */
    public void dropGraph(C conf, RyaURI... graphs) throws RyaDAOException; 

    /**
     * Delete a collection of RyaStatements.
     *
     * @param statements
     * @param conf
     * @throws RyaDAOException
     */
    public void delete(Iterator<RyaStatement> statements, C conf) throws RyaDAOException;

    /**
     * Get the version of the store.
     *
     * @return
     * @throws RyaDAOException
     */
    public String getVersion() throws RyaDAOException;

    /**
     * Get the Rya query engine
     * @return
     */
    public RyaQueryEngine<C> getQueryEngine();

    /**
     * Get the Rya Namespace Manager
     * @return
     */
    public RyaNamespaceManager<C> getNamespaceManager();

    public void purge(RdfCloudTripleStoreConfiguration configuration);

    public void dropAndDestroy() throws RyaDAOException;
}
