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
package org.apache.rya.api.client;

import java.util.Set;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Create a new PCJ within the target instance of Rya.
 */
@DefaultAnnotation(NonNull.class)
public interface CreatePCJ {
    
    /**
     * Metadata enum used to indicate the type of query that is registered.  If
     * the topmost node is a Construct QueryNode, then the type is Construct.  If the
     * topmost node is a Projection QueryNode, then the type is Projection.  If the
     * query contains a PeriodicQuery Filter anywhere within the query, then it is of type
     * Periodic. 
     *
     */
    public static enum QueryType{CONSTRUCT, PROJECTION, PERIODIC};
    
    /**
     * Specifies the how Results will be exported from the Rya Fluo
     * Application.
     *
     */
    public static enum ExportStrategy{RYA, KAFKA, PERIODIC};

    
    /**
     * Designate a new PCJ that will be maintained by the target instance of Rya.
     * Results will be exported according to the specified export strategies.
     *
     * @param instanceName - Indicates which Rya instance will create and maintain
     *   the PCJ. (not null)
     * @param sparql - The SPARQL query that will be maintained. (not null)
     * @param strategies - The export strategies used to export results for this query
     * @return The ID that was assigned to this newly created PCJ.
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws RyaClientException Something caused the command to fail.
     */
    public String createPCJ(final String instanceName, String sparql, Set<ExportStrategy> strategies) throws InstanceDoesNotExistException, RyaClientException;
    
    
    /**
     * Designate a new PCJ that will be maintained by the target instance of Rya.
     * Results will be exported to a Rya PCJ table.
     *
     * @param instanceName - Indicates which Rya instance will create and maintain
     *   the PCJ. (not null)
     * @param sparql - The SPARQL query that will be maintained. (not null)
     * @return The ID that was assigned to this newly created PCJ.
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws RyaClientException Something caused the command to fail.
     */
    public String createPCJ(final String instanceName, String sparql) throws InstanceDoesNotExistException, RyaClientException;
    
}