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

import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.mongodb.MongoTestBase;
import org.junit.Test;

import com.mongodb.MongoException;

/**
 * Integration tests the methods of {@link }.
 */
public class MongoExecuteSparqlQueryIT extends MongoTestBase {
    @Test
    public void ExecuteSparqlQuery_exec() throws MongoException, DuplicateInstanceNameException, RyaClientException {
        MongoConnectionDetails connectionDetails = getConnectionDetails();
        // Install a few instances of Rya using the install command.
        final Install install = new MongoInstall(connectionDetails, getMongoClient());
        install.install("instanceExec", InstallConfiguration.builder().build());
        MongoExecuteSparqlQuery executeSparql = new MongoExecuteSparqlQuery(connectionDetails, getMongoClient());
        // TODO executeSparql.
    }

    /**
     * @return copy from conf to MongoConnectionDetails
     */
    private MongoConnectionDetails getConnectionDetails() {
        return new MongoConnectionDetails(conf.getMongoUser(), conf.getMongoPassword().toCharArray(), conf.getMongoInstance(), Integer.parseInt(conf.getMongoPort()));
    }
}