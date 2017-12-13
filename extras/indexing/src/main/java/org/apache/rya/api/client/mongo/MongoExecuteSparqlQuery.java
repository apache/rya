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
package org.apache.rya.api.client.mongo;

import static java.util.Objects.requireNonNull;

import org.apache.rya.api.client.ExecuteSparqlQuery;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;

import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * TODO impl, test, doc
 */
@DefaultAnnotation(NonNull.class)
public class MongoExecuteSparqlQuery extends MongoCommand implements ExecuteSparqlQuery {

    /**
     * Constructs an instance of {@link }.
     *
     * @param connectionDetails - Details about the values that were used to create the client. (not null)
     * @param connector - Provides programmatic access to the instance of Mongo that hosts Rya instances. (not null)
     */
    public MongoExecuteSparqlQuery(final MongoConnectionDetails connectionDetails, final MongoClient client) {
        super(connectionDetails, client);
    }

    @Override
    public String executeSparqlQuery(final String ryaInstanceName, final String sparqlQuery) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(sparqlQuery);

        // TODO Auto-generated method stub
        return null;
    }
}