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
package org.apache.rya.indexing.pcj.storage;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.BindingSet;

/**
 * Interface for storing and retrieving Periodic Query Results.
 *
 */
public interface PeriodicQueryResultStorage {

    /**
     * Binding name for the periodic bin id
     */
    public static String PeriodicBinId = "periodicBinId";

    /**
     * Creates a PeriodicQuery result storage layer for the given SPARQL query
     * @param sparql - SPARQL query
     * @return - id of the storage layer for the given SPARQL query
     * @throws PeriodicQueryStorageException
     */
    public String createPeriodicQuery(String sparql) throws PeriodicQueryStorageException;

    /**
     * Creates a PeriodicQuery result storage layer for the given SPARQL query with the given id
     * @param queryId - id of the storage layer for the given SPARQL query
     * @param sparql - SPARQL query whose periodic results will be stored
     * @return - id of the storage layer
     * @throws PeriodicQueryStorageException
     */
    public String createPeriodicQuery(String queryId, String sparql) throws PeriodicQueryStorageException;

    /**
     * Creates a PeriodicQuery result storage layer for the given SPARQL query with the given id
     * whose results are written in the order indicated by the specified VariableOrder.
     * @param queryId - id of the storage layer for the given SPARQL query
     * @param sparql - SPARQL query whose periodic results will be stored
     * @param varOrder - VariableOrder indicating the order that results will be written in
     * @return - id of the storage layer
     * @throws PeriodicQueryStorageException
     */
    public void createPeriodicQuery(String queryId, String sparql, VariableOrder varOrder) throws PeriodicQueryStorageException;

    /**
     * Retrieve the {@link PeriodicQueryStorageMetdata} for the give query id
     * @param queryID - id of the query whose metadata will be returned
     * @return PeriodicQueryStorageMetadata
     * @throws PeriodicQueryStorageException
     */
    public PeriodicQueryStorageMetadata getPeriodicQueryMetadata(String queryID) throws PeriodicQueryStorageException;;

    /**
     * Add periodic query results to the storage layer indicated by the given query id
     * @param queryId - id indicating the storage layer that results will be added to
     * @param results - query results to be added to storage
     * @throws PeriodicQueryStorageException
     */
    public void addPeriodicQueryResults(String queryId, Collection<VisibilityBindingSet> results) throws PeriodicQueryStorageException;;

    /**
     * Deletes periodic query results from the storage layer
     * @param queryId - id indicating the storage layer that results will be deleted from
     * @param binID - bin id indicating the periodic id of results to be deleted
     * @throws PeriodicQueryStorageException
     */
    public void deletePeriodicQueryResults(String queryId, long binID) throws PeriodicQueryStorageException;;

    /**
     * Deletes all results for the storage layer indicated by the given query id
     * @param queryID - id indicating the storage layer whose results will be deleted
     * @throws PeriodicQueryStorageException
     */
    public void deletePeriodicQuery(String queryID) throws PeriodicQueryStorageException;;

    /**
     * List results in the given storage layer indicated by the query id
     * @param queryId - id indicating the storage layer whose results will be listed
     * @param binID - Optional id to indicate that only results with specific periodic id be listed
     * @return
     * @throws PeriodicQueryStorageException
     */
    public CloseableIterator<BindingSet> listResults(String queryId, Optional<Long> binID) throws PeriodicQueryStorageException;;

    /**
     * List all storage tables containing periodic results.
     * @return List of Strings with names of all tables containing periodic results
     */
    public List<String> listPeriodicTables();

}
