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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.LoadStatements;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Accumulo implementation of the {@link LoadStatements} command.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloLoadStatements extends AccumuloCommand implements LoadStatements {
    private static final Logger log = Logger.getLogger(AccumuloLoadStatements.class);

    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link AccumuloLoadStatements}.
     *
     * @param connectionDetails - Details about the values that were used to create
     *   the connector to the cluster. (not null)
     * @param connector - Provides programmatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     */
    public AccumuloLoadStatements(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        instanceExists = new AccumuloInstanceExists(connectionDetails, connector);
    }

    @Override
    public void loadStatements(final String ryaInstanceName, final Iterable<? extends Statement> statements) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(statements);

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
            ryaConf.setFlush(false); //RYA-327 should address this hardcoded value.
            sail = RyaSailFactory.getInstance(ryaConf);

            // Load the file.
            sailRepo = new SailRepository(sail);
            sailRepoConn = sailRepo.getConnection();
            sailRepoConn.add(statements);

        } catch (final SailException | AccumuloException | AccumuloSecurityException | RyaDAOException | InferenceEngineException  e) {
            log.warn("Exception while loading:", e);
            throw new RyaClientException("A problem connecting to the Rya instance named '" + ryaInstanceName + "' has caused the load to fail.", e);
        } catch (final Exception e) {
            log.warn("Exception while loading:", e);
            throw new RyaClientException("A problem processing the RDF statements has caused the load into Rya instance named " + ryaInstanceName + "to fail.", e);
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
}