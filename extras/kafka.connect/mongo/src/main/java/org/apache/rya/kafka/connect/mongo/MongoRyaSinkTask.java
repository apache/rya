/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.kafka.connect.mongo;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.mongo.MongoConnectionDetails;
import org.apache.rya.api.client.mongo.MongoRyaClientFactory;
import org.apache.rya.api.log.LogUtils;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.kafka.connect.api.sink.RyaSinkTask;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;

import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link RyaSinkTask} that uses the Mongo DB implementation of Rya to store data.
 */
@DefaultAnnotation(NonNull.class)
public class MongoRyaSinkTask extends RyaSinkTask {

    @Override
    protected void checkRyaInstanceExists(final Map<String, String> taskConfig) throws IllegalStateException {
        requireNonNull(taskConfig);

        // Parse the configuration object.
        final MongoRyaSinkConfig config = new MongoRyaSinkConfig(taskConfig);
        @Nullable
        final String username = Strings.isNullOrEmpty(config.getUsername()) ? null : config.getUsername();
        @Nullable
        final char[] password = Strings.isNullOrEmpty(config.getPassword()) ? null : config.getPassword().toCharArray();

        // Connect a Mongo Client to the configured Mongo DB instance.
        final ServerAddress serverAddr = new ServerAddress(config.getHostname(), config.getPort());
        final boolean hasCredentials = username != null && password != null;

        try(MongoClient mongoClient = hasCredentials ?
                new MongoClient(serverAddr, Arrays.asList(MongoCredential.createCredential(username, config.getRyaInstanceName(), password))) :
                new MongoClient(serverAddr)) {
            // Use a RyaClient to see if the configured instance exists.
            // Create the Mongo Connection Details that describe the Mongo DB Server we are interacting with.
            final MongoConnectionDetails connectionDetails = new MongoConnectionDetails(
                    config.getHostname(),
                    config.getPort(),
                    Optional.ofNullable(username),
                    Optional.ofNullable(password));

            final RyaClient client = MongoRyaClientFactory.build(connectionDetails, mongoClient);
            if(!client.getInstanceExists().exists( config.getRyaInstanceName() )) {
                throw new ConnectException("The Rya Instance named " +
                        LogUtils.clean(config.getRyaInstanceName()) + " has not been installed.");
            }
        } catch(final RyaClientException e) {
            throw new ConnectException("Unable to determine if the Rya Instance named " +
                    LogUtils.clean(config.getRyaInstanceName()) + " has been installed.", e);
        }
    }

    @Override
    protected Sail makeSail(final Map<String, String> taskConfig) {
        requireNonNull(taskConfig);

        // Parse the configuration object.
        final MongoRyaSinkConfig config = new MongoRyaSinkConfig(taskConfig);

        // Move the configuration into a Rya Configuration object.
        final MongoDBRdfConfiguration ryaConfig = new MongoDBRdfConfiguration();
        ConfigUtils.setUseMongo(ryaConfig, true);
        ryaConfig.setRyaInstanceName( config.getRyaInstanceName() );
        ryaConfig.setMongoHostname( config.getHostname() );
        ryaConfig.setMongoPort( "" + config.getPort() );

        if(!Strings.isNullOrEmpty(config.getUsername()) && !Strings.isNullOrEmpty(config.getPassword())) {
            ryaConfig.setMongoUser( config.getUsername() );
            ryaConfig.setMongoPassword( config.getPassword() );
        }

        // Create the Sail object.
        try {
            return RyaSailFactory.getInstance(ryaConfig);
        } catch (SailException | AccumuloException | AccumuloSecurityException | RyaDAOException | InferenceEngineException e) {
            throw new ConnectException("Could not connect to the Rya Instance named " + config.getRyaInstanceName(), e);
        }
    }
}