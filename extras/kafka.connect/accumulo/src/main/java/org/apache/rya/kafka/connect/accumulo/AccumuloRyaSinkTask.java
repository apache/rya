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
package org.apache.rya.kafka.connect.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.api.log.LogUtils;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.kafka.connect.api.sink.RyaSinkTask;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link RyaSinkTask} that uses the Accumulo implementation of Rya to store data.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloRyaSinkTask extends RyaSinkTask {

    @Override
    protected void checkRyaInstanceExists(final Map<String, String> taskConfig) throws ConnectException {
        requireNonNull(taskConfig);

        // Parse the configuration object.
        final AccumuloRyaSinkConfig config = new AccumuloRyaSinkConfig(taskConfig);

        // Connect to the instance of Accumulo.
        final Connector connector;
        try {
            final Instance instance = new ZooKeeperInstance(config.getClusterName(), config.getZookeepers());
            connector = instance.getConnector(config.getUsername(), new PasswordToken( config.getPassword() ));
        } catch (final AccumuloException | AccumuloSecurityException e) {
            throw new ConnectException("Could not create a Connector to the configured Accumulo instance.", e);
        }

        // Use a RyaClient to see if the configured instance exists.
        try {
            final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                    config.getUsername(),
                    config.getPassword().toCharArray(),
                    config.getClusterName(),
                    config.getZookeepers());
            final RyaClient client = AccumuloRyaClientFactory.build(connectionDetails, connector);

            if(!client.getInstanceExists().exists( config.getRyaInstanceName() )) {
                throw new ConnectException("The Rya Instance named " +
                        LogUtils.clean(config.getRyaInstanceName()) + " has not been installed.");
            }

        } catch (final RyaClientException e) {
            throw new ConnectException("Unable to determine if the Rya Instance named " +
                    LogUtils.clean(config.getRyaInstanceName()) + " has been installed.", e);
        }
    }

    @Override
    protected Sail makeSail(final Map<String, String> taskConfig) throws ConnectException {
        requireNonNull(taskConfig);

        // Parse the configuration object.
        final AccumuloRyaSinkConfig config = new AccumuloRyaSinkConfig(taskConfig);

        // Move the configuration into a Rya Configuration object.
        final AccumuloRdfConfiguration ryaConfig = new AccumuloRdfConfiguration();
        ryaConfig.setTablePrefix( config.getRyaInstanceName() );
        ryaConfig.setAccumuloZookeepers( config.getZookeepers() );
        ryaConfig.setAccumuloInstance( config.getClusterName() );
        ryaConfig.setAccumuloUser( config.getUsername() );
        ryaConfig.setAccumuloPassword( config.getPassword() );
        ryaConfig.setFlush(false);

        // Create the Sail object.
        try {
            return RyaSailFactory.getInstance(ryaConfig);
        } catch (SailException | AccumuloException | AccumuloSecurityException | RyaDAOException | InferenceEngineException e) {
            throw new ConnectException("Could not connect to the Rya Instance named " + config.getRyaInstanceName(), e);
        }
    }
}