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
import java.nio.file.Path;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.LoadStatementsFile;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Mongo implementation of the {@link LoadStatementsFile} command.
 */
@DefaultAnnotation(NonNull.class)
public class MongoLoadStatementsFile implements LoadStatementsFile {
    private static final Logger log = LoggerFactory.getLogger(MongoLoadStatementsFile.class);

    private final MongoConnectionDetails connectionDetails;
    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link MongoListInstances}.
     *
     * @param connectionDetails - Details to connect to the server. (not null)
     * @param instanceExists - The interactor used to check if a Rya instance exists. (not null)
     */
    public MongoLoadStatementsFile(
            final MongoConnectionDetails connectionDetails,
            final MongoInstanceExists instanceExists) {
        this.connectionDetails = requireNonNull(connectionDetails);
        this.instanceExists = requireNonNull(instanceExists);
    }

    @Override
    public void loadStatements(final String ryaInstanceName, final Path statementsFile, final RDFFormat format) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(statementsFile);
        requireNonNull(format);

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

            // Load the file.
            sailRepoConn.add(statementsFile.toFile(), null, format);

        } catch (SailException | RyaDAOException | InferenceEngineException | AccumuloException | AccumuloSecurityException e) {
            throw new RyaClientException("Could not load statements into Rya because of a problem while creating the Sail object.", e);
        } catch (RDFParseException | RepositoryException | IOException e) {
            throw new RyaClientException("Could not load the statements into Rya.", e);
        } finally {
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
}