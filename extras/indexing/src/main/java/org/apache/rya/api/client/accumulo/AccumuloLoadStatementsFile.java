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
package org.apache.rya.api.client.accumulo;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.LoadStatementsFile;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Accumulo implementation of the {@link LoadStatementsFile} command.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloLoadStatementsFile extends AccumuloCommand implements LoadStatementsFile {
    private static final Logger log = Logger.getLogger(AccumuloLoadStatementsFile.class);

    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link AccumuloLoadStatementsFile}.
     *
     * @param connectionDetails - Details about the values that were used to create
     *   the connector to the cluster. (not null)
     * @param connector - Provides programmatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     */
    public AccumuloLoadStatementsFile(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        instanceExists = new AccumuloInstanceExists(connectionDetails, connector);
    }

    @Override
    public void loadStatements(final String ryaInstanceName, final Path statementsFile, final RDFFormat format) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(statementsFile);
        requireNonNull(format);

        // Ensure the Rya Instance exists.
        if(!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }

        Sail sail = null;
        SailRepository sailRepo = null;
        SailRepositoryConnection sailRepoConn = null;

        try {
            // Get a Sail object that is connected to the Rya instance.
            final AccumuloConnectionDetails connDetails = getAccumuloConnectionDetails();

            final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();
            ryaConf.setTablePrefix( ryaInstanceName );
            ryaConf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, connDetails.getZookeepers());
            ryaConf.set(ConfigUtils.CLOUDBASE_INSTANCE, connDetails.getInstanceName());
            ryaConf.set(ConfigUtils.CLOUDBASE_USER, connDetails.getUsername());
            ryaConf.set(ConfigUtils.CLOUDBASE_PASSWORD, new String(connDetails.getPassword()));

            sail = RyaSailFactory.getInstance(ryaConf);

            // Load the file.
            sailRepo = new SailRepository( sail );
            sailRepoConn = sailRepo.getConnection();
            sailRepoConn.add(statementsFile.toFile(), null, format);

        } catch (final SailException | AccumuloException | AccumuloSecurityException | RyaDAOException | InferenceEngineException  e) {
            throw new RyaClientException("A problem connecting to the Rya instance named '" + ryaInstanceName + "' has caused the load to fail.", e);
        } catch (final RepositoryException | RDFParseException | IOException e) {
            throw new RyaClientException("A problem processing the RDF file has caused the load into Rya instance named " + ryaInstanceName + "to fail.", e);
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