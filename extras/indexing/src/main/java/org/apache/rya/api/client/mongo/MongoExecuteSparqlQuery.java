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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;

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
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.text.csv.SPARQLResultsCSVWriter;
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
    public String executeSparqlQuery(final String ryaInstanceName, final String sparqlQuery) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(sparqlQuery);

        // Ensure the Rya Instance exists.
        if (!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }

        Sail sail = null;
        SailRepositoryConnection sailRepoConn = null;
        try {
            // Get a Sail object that is connected to the Rya instance.
            final MongoDBRdfConfiguration ryaConf = connectionDetails.build(ryaInstanceName);
            sail = RyaSailFactory.getInstance(ryaConf);

            final SailRepository sailRepo = new SailRepository(sail);
            sailRepoConn = sailRepo.getConnection();

            // Execute the query.
            final long start = System.currentTimeMillis();
            final TupleQuery tupleQuery = sailRepoConn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final CountingSPARQLResultsCSVWriter handler = new CountingSPARQLResultsCSVWriter(baos);
            tupleQuery.evaluate(handler);
            final long end = System.currentTimeMillis();

            // Format and return the result of the query.
            final String queryResult = new String(baos.toByteArray(), StandardCharsets.UTF_8);
            final String queryDuration = new DecimalFormat("0.0##").format((end - start) / 1000.0);

            return "Query Result:\n" +
                    queryResult +
                    "Retrieved " + handler.getCount() + " results in " + queryDuration + " seconds.";

        } catch (SailException | RyaDAOException | InferenceEngineException | AccumuloException | AccumuloSecurityException e) {
            throw new RyaClientException("Could not create the Sail object used to query the RYA instance.", e);
        } catch (final MalformedQueryException | QueryEvaluationException | TupleQueryResultHandlerException | RepositoryException e) {
            throw new RyaClientException("Could not execute the SPARQL query.", e);
        }  finally {
            // Close the resources that were opened.
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

    /**
     * Subclasses {@link SPARQLResultsCSVWriter} to keep track of the total count of handled {@link BindingSet} objects.
     */
    private static class CountingSPARQLResultsCSVWriter extends SPARQLResultsCSVWriter {

        private int count = 0;

        /**
         * @param out - The OutputStream for results to be written to.
         */
        public CountingSPARQLResultsCSVWriter(final OutputStream out) {
            super(out);
        }

        @Override
        public void handleSolution(final BindingSet bindingSet) throws TupleQueryResultHandlerException {
            super.handleSolution(bindingSet);
            count++;
        }

        /**
         *
         * @return The number of BindingSets that were handled by {@link #handleSolution(BindingSet)}.
         */
        public int getCount() {
            return count;
        }
    }
}