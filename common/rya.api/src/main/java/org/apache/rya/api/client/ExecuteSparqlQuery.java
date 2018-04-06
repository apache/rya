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

import java.io.Closeable;

import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Loads a SPARQL Query and executes the query against an instance of Rya.
 */
@DefaultAnnotation(NonNull.class)
public interface ExecuteSparqlQuery extends Closeable {

    /**
     * Executes a SPARQL Query against an instance of Rya.
     *
     * @param ryaInstanceName - The name of the Rya instance the query will be executed against. (not null)
     * @param sparqlQuery - A SPARQL Query. (not null)
     * @return A {@link TupleQueryResult} of the resulting {@link BindingSet}s.
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws RyaClientException Something caused the command to fail.
     */
    public TupleQueryResult executeSparqlQuery(String ryaInstanceName, String sparqlQuery) throws InstanceDoesNotExistException, RyaClientException;
}