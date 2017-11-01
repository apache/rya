/*
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
package org.apache.rya.api.client.accumulo;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.ExecuteSparqlQuery;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.resultio.text.csv.SPARQLResultsCSVWriter;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Accumulo implementation of the {@link ExecuteSparqlQuery} command.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloExecuteSparqlQuery extends AccumuloCommand implements ExecuteSparqlQuery {
    private static final Logger log = Logger.getLogger(AccumuloExecuteSparqlQuery.class);

    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link AccumuloExecuteSparqlQuery}.
     *
     * @param connectionDetails - Details about the values that were used to create
     *   the connector to the cluster. (not null)
     * @param connector - Provides programmatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     */
    public AccumuloExecuteSparqlQuery(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        instanceExists = new AccumuloInstanceExists(connectionDetails, connector);
    }


    @Override
    public String executeSparqlQuery(final String ryaInstanceName, final String sparqlQuery)
            throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(sparqlQuery);

        // Ensure the Rya Instance exists.
        if(!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }


        Sail sail = null;
        SailRepository sailRepo = null;
        SailRepositoryConnection sailRepoConn = null;

        try {
            // Get a Sail object that is connected to the Rya instance.
            final AccumuloRdfConfiguration ryaConf = getAccumuloConnectionDetails().buildAccumuloRdfConfiguration(ryaInstanceName);
            sail = RyaSailFactory.getInstance(ryaConf);
            sailRepo = new SailRepository(sail);
            sailRepoConn = sailRepo.getConnection();

            // Execute the query.
            final long start = System.currentTimeMillis();
            final TupleQuery tupleQuery = sailRepoConn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final CountingSPARQLResultsCSVWriter handler = new CountingSPARQLResultsCSVWriter(baos);
            tupleQuery.evaluate(handler);
            final StringBuilder sb = new StringBuilder();

            final String newline = "\n";
            sb.append("Query Result:").append(newline);
            sb.append(new String(baos.toByteArray(), StandardCharsets.UTF_8));

            final String seconds = new DecimalFormat("0.0##").format((System.currentTimeMillis() - start) / 1000.0);
            sb.append("Retrieved ").append(handler.getCount()).append(" results in ").append(seconds).append(" seconds.");

            return sb.toString();

        } catch (final SailException | AccumuloException | AccumuloSecurityException | RyaDAOException | InferenceEngineException  e) {
            throw new RyaClientException("A problem connecting to the Rya instance named '" + ryaInstanceName + "' has caused the query to fail.", e);
        } catch (final MalformedQueryException e) {
            throw new RyaClientException("There was a problem parsing the supplied query.", e);
        } catch (final QueryEvaluationException | TupleQueryResultHandlerException e) {
            throw new RyaClientException("There was a problem evaluating the supplied query.", e);
        } catch (final RepositoryException e) {
            throw new RyaClientException("There was a problem executing the query against the Rya instance named " + ryaInstanceName + ".", e);
        } finally {
            // Shut it all down.
            if(sailRepoConn != null) {
                try {
                    sailRepoConn.close();
                } catch (final RepositoryException e) {
                    log.warn("Couldn't close the SailRepoConnection that is attached to the Rya instance.", e);
                }
            }
            if(sailRepo != null) {
                try {
                    sailRepo.shutDown();
                } catch (final RepositoryException e) {
                    log.warn("Couldn't shut down the SailRepository that is attached to the Rya instance.", e);
                }
            }
            if(sail != null) {
                try {
                    sail.shutDown();
                } catch (final SailException e) {
                    log.warn("Couldn't shut down the Sail that is attached to the Rya instance.", e);
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