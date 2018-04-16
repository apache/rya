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

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.rya.api.client.ExecuteSparqlQuery;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Execute a sparql query on mongo Rya.
 */
@DefaultAnnotation(NonNull.class)
public class MongoExecuteSparqlQuery implements ExecuteSparqlQuery {
    private static final Logger log = LoggerFactory.getLogger(MongoExecuteSparqlQuery.class);

    private final MongoConnectionDetails connectionDetails;
    private final InstanceExists instanceExists;
    private Sail sail = null;
    private SailRepositoryConnection sailRepoConn = null;

    /**
     * Constructs an instance of {@link MongoExecuteSparqlQuery}.
     *
     * @param connectionDetails - Details to connect to the server. (not null)
     * @param instanceExists - The interactor used to check if a Rya instance exists. (not null)
     */
    public MongoExecuteSparqlQuery(
            final MongoConnectionDetails connectionDetails,
            final MongoInstanceExists instanceExists) {
        this.connectionDetails = requireNonNull(connectionDetails);
        this.instanceExists = requireNonNull(instanceExists);
    }

    @Override
    public TupleQueryResult executeSparqlQuery(final String ryaInstanceName, final String sparqlQuery) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(sparqlQuery);

        // Ensure the Rya Instance exists.
        if (!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }

        try {
            // Get a Sail object that is connected to the Rya instance.
            final MongoDBRdfConfiguration ryaConf = connectionDetails.build(ryaInstanceName);
            sail = RyaSailFactory.getInstance(ryaConf);

            final SailRepository sailRepo = new SailRepository(sail);
            sailRepoConn = sailRepo.getConnection();

            // Execute the query.
            final TupleQuery tupleQuery = sailRepoConn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
            return tupleQuery.evaluate();
        } catch (SailException | RyaDAOException | InferenceEngineException | AccumuloException | AccumuloSecurityException e) {
            throw new RyaClientException("Could not create the Sail object used to query the RYA instance.", e);
        } catch (final MalformedQueryException | QueryEvaluationException | RepositoryException e) {
            throw new RyaClientException("Could not execute the SPARQL query.", e);
        }
    }

    @Override
    public void close() throws IOException {
        if(sailRepoConn != null) {
            try {
                sailRepoConn.close();
            } catch (final RepositoryException e) {
                log.error("Couldn't close the SailRepositoryConnection object.", e);
            }
        }

        if(sail != null) {
            try {
                sail.shutDown();
            } catch (final SailException e) {
                log.error("Couldn't close the Sail object.", e);
            }
        }
    }
}