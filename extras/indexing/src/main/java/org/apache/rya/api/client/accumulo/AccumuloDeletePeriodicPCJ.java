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
import org.apache.rya.api.client.DeletePeriodicPCJ;
import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import org.apache.rya.indexing.pcj.fluo.api.DeletePeriodicQuery;
import org.apache.rya.indexing.pcj.fluo.api.DeletePeriodicQuery.QueryDeletionException;
import org.apache.rya.indexing.pcj.fluo.app.query.UnsupportedQueryException;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.periodic.notification.api.PeriodicNotificationClient;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.registration.KafkaNotificationRegistrationClient;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * Class used by the RyaClient and Rya Shell for deleting Periodic PCJ.
 *
 */
public class AccumuloDeletePeriodicPCJ extends AccumuloCommand implements DeletePeriodicPCJ {
    private static final Logger log = LoggerFactory.getLogger(AccumuloDeletePCJ.class);

    private final GetInstanceDetails getInstanceDetails;

    /**
     * Constructs an instance of {@link AccumuloDeletePeriodicPCJ}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programmatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloDeletePeriodicPCJ(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        getInstanceDetails = new AccumuloGetInstanceDetails(connectionDetails, connector);
    }

    @Override
    public void deletePeriodicPCJ(final String instanceName, final String pcjId, String topic, String brokers) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(instanceName);
        requireNonNull(pcjId);

        final Optional<RyaDetails> originalDetails = getInstanceDetails.getDetails(instanceName);
        final boolean ryaInstanceExists = originalDetails.isPresent();
        if(!ryaInstanceExists) {
            throw new InstanceDoesNotExistException(String.format("The '%s' instance of Rya does not exist.", instanceName));
        }

        final boolean pcjIndexingEnabled = originalDetails.get().getPCJIndexDetails().isEnabled();
        if(!pcjIndexingEnabled) {
            throw new RyaClientException(String.format("The '%s' instance of Rya does not have PCJ Indexing enabled.", instanceName));
        }

        // If the PCJ was being maintained by a Fluo application, then stop that process.
        final PCJIndexDetails pcjIndexDetails = originalDetails.get().getPCJIndexDetails();
        final Optional<FluoDetails> fluoDetailsHolder = pcjIndexDetails.getFluoDetails();

        if (fluoDetailsHolder.isPresent()) {
            final String fluoAppName = pcjIndexDetails.getFluoDetails().get().getUpdateAppName();
            try {
                stopUpdatingPCJ(instanceName, fluoAppName, pcjId, topic, brokers);
            } catch (MalformedQueryException | UnsupportedQueryException | QueryDeletionException e) {
                throw new RyaClientException(String.format("Unable to delete Periodic Query with id: %s", pcjId), e);
            }
        } else {
            log.error(String.format("Could not stop the Fluo application from updating the PCJ because the Fluo Details are "
                    + "missing for the Rya instance named '%s'.", instanceName));
        }
        
    }


    private void stopUpdatingPCJ(final String ryaInstance, final String fluoAppName, final String pcjId, final String topic, final String brokers) throws UnsupportedQueryException, MalformedQueryException, QueryDeletionException {
        requireNonNull(fluoAppName);
        requireNonNull(pcjId);

        // Connect to the Fluo application that is updating this instance's PCJs.
        final AccumuloConnectionDetails cd = super.getAccumuloConnectionDetails();
        try (final FluoClient fluoClient = new FluoClientFactory().connect(cd.getUsername(), new String(cd.getUserPass()),
                cd.getInstanceName(), cd.getZookeepers(), fluoAppName)) {
            // Delete the PCJ from the Fluo App.
            PeriodicQueryResultStorage periodic = new AccumuloPeriodicQueryResultStorage(getConnector(), ryaInstance);
            DeletePeriodicQuery deletePeriodic = new DeletePeriodicQuery(fluoClient, periodic);
            deletePeriodic.deletePeriodicQuery(pcjId, getPeriodicNotificationClient(topic, brokers));
        }
    }
    
    
    private static PeriodicNotificationClient getPeriodicNotificationClient(String topic, String brokers) throws MalformedQueryException {
        return new KafkaNotificationRegistrationClient(topic, createProducer(brokers));
    }

    private static KafkaProducer<String, CommandNotification> createProducer(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommandNotificationSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
    
}
