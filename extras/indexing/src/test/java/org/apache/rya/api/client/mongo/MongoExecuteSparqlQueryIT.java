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

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.rya.api.client.ExecuteSparqlQuery;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.mongodb.MongoTestBase;
import org.junit.Test;
import org.openrdf.model.Statement;

import com.mongodb.MongoException;

/**
 * Integration tests the methods of {@link }.
 */
public class MongoExecuteSparqlQueryIT extends MongoTestBase {
    @Test
    public void ExecuteSparqlQuery_exec() throws MongoException, DuplicateInstanceNameException, RyaClientException {
        // Install an instance of Rya.
        MongoConnectionDetails connectionDetails = getConnectionDetails();
        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, conf.getMongoClient());
        // install rya and load some data
        final List<Statement> loadMe = MongoLoadStatementsIT.installAndLoad();
        // Here comes the method to test
        ExecuteSparqlQuery executeSparql = ryaClient.getExecuteSparqlQuery();
        final String sparql = "SELECT * where { ?a ?b ?c }";
        String results = executeSparql.executeSparqlQuery(conf.getMongoDBName(), sparql);
        System.out.println(results);
        assertTrue("result has header.", results.startsWith("Query Result:"));
        assertTrue("result has column headings.", results.contains("a,b,c"));
        assertTrue("result has footer.", results.contains("Retrieved 3 results in"));
        for (Statement expect : loadMe) {
            assertTrue("All results should contain expected subjects:",
                            results.contains(expect.getSubject().stringValue()));
            assertTrue("All results should contain expected predicates:",
                            results.contains(expect.getPredicate().stringValue()));
            assertTrue("All results should contain expected objects:",
                            results.contains(expect.getObject().stringValue()));
        }
    }

    /**
     * @return copy from conf to MongoConnectionDetails
     */
    private MongoConnectionDetails getConnectionDetails() {
        return new MongoConnectionDetails(
                        conf.getMongoUser(),
                        conf.getMongoPassword().toCharArray(),
                        conf.getMongoInstance(),
                        Integer.parseInt(conf.getMongoPort()));

    }
}