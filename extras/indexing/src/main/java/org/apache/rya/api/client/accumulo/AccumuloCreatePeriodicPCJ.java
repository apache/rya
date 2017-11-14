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

import java.util.Properties;

import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.client.CreatePeriodicPCJ;
import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.pcj.fluo.api.CreatePeriodicQuery;
import org.apache.rya.indexing.pcj.fluo.api.CreatePeriodicQuery.PeriodicQueryCreationException;
import org.apache.rya.indexing.pcj.fluo.app.query.UnsupportedQueryException;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.periodic.notification.api.PeriodicNotificationClient;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.registration.KafkaNotificationRegistrationClient;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.sail.SailException;

import com.google.common.base.Optional;

/**
 * Class used by the RyaClient for creating Periodic PCJ.
 *
 */
public class AccumuloCreatePeriodicPCJ extends AccumuloCommand implements CreatePeriodicPCJ {

    private final GetInstanceDetails getInstanceDetails;
    
    /**
     * Constructs an instance of {@link AccumuloCreatePeriodicPCJ}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloCreatePeriodicPCJ(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        getInstanceDetails = new AccumuloGetInstanceDetails(connectionDetails, connector);
    }
    
    @Override
    public String createPeriodicPCJ(String instanceName, String sparql, String periodicTopic, String bootStrapServers) throws RyaClientException {
        requireNonNull(instanceName);
        requireNonNull(sparql);

        final Optional<RyaDetails> ryaDetailsHolder = getInstanceDetails.getDetails(instanceName);
        final boolean ryaInstanceExists = ryaDetailsHolder.isPresent();
        if (!ryaInstanceExists) {
            throw new InstanceDoesNotExistException(String.format("The '%s' instance of Rya does not exist.", instanceName));
        }

        final PCJIndexDetails pcjIndexDetails = ryaDetailsHolder.get().getPCJIndexDetails();
        final boolean pcjIndexingEnabeld = pcjIndexDetails.isEnabled();
        if (!pcjIndexingEnabeld) {
            throw new RyaClientException(String.format("The '%s' instance of Rya does not have PCJ Indexing enabled.", instanceName));
        }

        // If a Fluo application is being used, task it with updating the PCJ.
        final Optional<FluoDetails> fluoDetailsHolder = pcjIndexDetails.getFluoDetails();
        if (fluoDetailsHolder.isPresent()) {
            final String fluoAppName = fluoDetailsHolder.get().getUpdateAppName();
            try {
                return updateFluoAppAndRegisterWithKafka(instanceName, fluoAppName, sparql, periodicTopic, bootStrapServers);
            } catch (RepositoryException | MalformedQueryException | SailException | QueryEvaluationException | PcjException
                    | RyaDAOException | PeriodicQueryCreationException e) {
                throw new RyaClientException("Problem while initializing the Fluo application with the new PCJ.", e);
            } catch (UnsupportedQueryException e) {
                throw new RyaClientException("The new PCJ could not be initialized because it either contains an unsupported query node "
                        + "or an invalid ExportStrategy for the given QueryType.  Projection queries can be exported to either Rya or Kafka,"
                        + "unless they contain an aggregation, in which case they can only be exported to Kafka.  Construct queries can be exported"
                        + "to Rya and Kafka, and Periodic queries can only be exported to Rya.");
            } 
        } else {
            throw new RyaClientException(String.format("The '%s' instance of Rya does not have PCJ Indexing enabled.", instanceName));
        }
    }


    
    
    private String updateFluoAppAndRegisterWithKafka(final String ryaInstance, final String fluoAppName, String sparql, String periodicTopic, String bootStrapServers) throws RepositoryException, MalformedQueryException, SailException, QueryEvaluationException, PcjException, RyaDAOException, UnsupportedQueryException, PeriodicQueryCreationException {
        requireNonNull(sparql);
        requireNonNull(periodicTopic);
        requireNonNull(bootStrapServers);

        final PeriodicQueryResultStorage periodicStorage = new AccumuloPeriodicQueryResultStorage(getConnector(), ryaInstance);
        
        // Connect to the Fluo application that is updating this instance's PCJs.
        final AccumuloConnectionDetails cd = super.getAccumuloConnectionDetails();
        try(final FluoClient fluoClient = new FluoClientFactory().connect(
                cd.getUsername(),
                new String(cd.getPassword()),
                cd.getInstanceName(),
                cd.getZookeepers(),
                fluoAppName)) {
            // Initialize the PCJ within the Fluo application.
            final CreatePeriodicQuery periodicPcj = new CreatePeriodicQuery(fluoClient, periodicStorage);
            PeriodicNotificationClient periodicClient = new KafkaNotificationRegistrationClient(periodicTopic, createProducer(bootStrapServers));
            return periodicPcj.withRyaIntegration(sparql, periodicClient, getConnector(), ryaInstance).getQueryId();
        }
    }
    
    
    private static KafkaProducer<String, CommandNotification> createProducer(String bootStrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommandNotificationSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

}
