/*
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
package org.apache.rya.periodic.notification.application;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoClientFactory;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.periodic.notification.api.BindingSetRecord;
import org.apache.rya.periodic.notification.api.NodeBin;
import org.apache.rya.periodic.notification.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.coordinator.PeriodicNotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.exporter.KafkaExporterExecutor;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.apache.rya.periodic.notification.processor.NotificationProcessorExecutor;
import org.apache.rya.periodic.notification.pruner.PeriodicQueryPrunerExecutor;
import org.apache.rya.periodic.notification.recovery.PeriodicNotificationProvider;
import org.apache.rya.periodic.notification.registration.kafka.KafkaNotificationProvider;
import org.apache.rya.periodic.notification.serialization.BindingSetSerDe;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.eclipse.rdf4j.query.BindingSet;


/**
 * Factory for creating a {@link PeriodicNotificationApplication}.
 */
public class PeriodicNotificationApplicationFactory {

    /**
     * Create a PeriodicNotificationApplication.
     * @param conf - Configuration object that specifies the parameters needed to create the application
     * @return PeriodicNotificationApplication to periodically poll Rya Fluo for new results
     * @throws PeriodicApplicationException
     */
    public static PeriodicNotificationApplication getPeriodicApplication(final PeriodicNotificationApplicationConfiguration conf) throws PeriodicApplicationException {
        final Properties kafkaConsumerProps = getKafkaConsumerProperties(conf);
        final Properties kafkaProducerProps = getKafkaProducerProperties(conf);

        final BlockingQueue<TimestampedNotification> notifications = new LinkedBlockingQueue<>();
        final BlockingQueue<NodeBin> bins = new LinkedBlockingQueue<>();
        final BlockingQueue<BindingSetRecord> bindingSets = new LinkedBlockingQueue<>();

        FluoClient fluo = null;
        try {
            final PeriodicQueryResultStorage storage = getPeriodicQueryResultStorage(conf);
            fluo = FluoClientFactory.getFluoClient(conf.getFluoAppName(), Optional.of(conf.getFluoTableName()), conf);
            final NotificationCoordinatorExecutor coordinator = getCoordinator(conf.getCoordinatorThreads(), notifications);
            addRegisteredNotices(coordinator, fluo.newSnapshot());
            final KafkaExporterExecutor exporter = getExporter(conf.getExporterThreads(), kafkaProducerProps, bindingSets);
            final PeriodicQueryPrunerExecutor pruner = getPruner(storage, fluo, conf.getPrunerThreads(), bins);
            final NotificationProcessorExecutor processor = getProcessor(storage, notifications, bins, bindingSets, conf.getProcessorThreads());
            final KafkaNotificationProvider provider = getProvider(conf.getProducerThreads(), conf.getNotificationTopic(), coordinator, kafkaConsumerProps);
            return PeriodicNotificationApplication.builder().setCoordinator(coordinator).setProvider(provider).setExporter(exporter)
                    .setProcessor(processor).setPruner(pruner).build();
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new PeriodicApplicationException(e.getMessage());
        }
    }

    private static void addRegisteredNotices(final NotificationCoordinatorExecutor coord, final Snapshot sx) {
        coord.start();
        final PeriodicNotificationProvider provider = new PeriodicNotificationProvider();
        provider.processRegisteredNotifications(coord, sx);
    }

    private static NotificationCoordinatorExecutor getCoordinator(final int numThreads, final BlockingQueue<TimestampedNotification> notifications) {
        return new PeriodicNotificationCoordinatorExecutor(numThreads, notifications);
    }

    private static KafkaExporterExecutor getExporter(final int numThreads, final Properties props, final BlockingQueue<BindingSetRecord> bindingSets) {
        final KafkaProducer<String, BindingSet> producer = new KafkaProducer<>(props, new StringSerializer(), new BindingSetSerDe());
        return new KafkaExporterExecutor(producer, numThreads, bindingSets);
    }

    private static PeriodicQueryPrunerExecutor getPruner(final PeriodicQueryResultStorage storage, final FluoClient fluo, final int numThreads,
            final BlockingQueue<NodeBin> bins) {
        return new PeriodicQueryPrunerExecutor(storage, fluo, numThreads, bins);
    }

    private static NotificationProcessorExecutor getProcessor(final PeriodicQueryResultStorage periodicStorage,
            final BlockingQueue<TimestampedNotification> notifications, final BlockingQueue<NodeBin> bins, final BlockingQueue<BindingSetRecord> bindingSets,
            final int numThreads) {
        return new NotificationProcessorExecutor(periodicStorage, notifications, bins, bindingSets, numThreads);
    }

    private static KafkaNotificationProvider getProvider(final int numThreads, final String topic, final NotificationCoordinatorExecutor coord,
            final Properties props) {
        return new KafkaNotificationProvider(topic, new StringDeserializer(), new CommandNotificationSerializer(), props, coord,
                numThreads);
    }

    private static PeriodicQueryResultStorage getPeriodicQueryResultStorage(final PeriodicNotificationApplicationConfiguration conf)
            throws AccumuloException, AccumuloSecurityException {
        final Instance instance = new ZooKeeperInstance(conf.getAccumuloInstance(), conf.getAccumuloZookeepers());
        final Connector conn = instance.getConnector(conf.getAccumuloUser(), new PasswordToken(conf.getAccumuloPassword()));
        final String ryaInstance = conf.getTablePrefix();
        return new AccumuloPeriodicQueryResultStorage(conn, ryaInstance);
    }

    private static Properties getKafkaConsumerProperties(final PeriodicNotificationApplicationConfiguration conf) {
        final Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getBootStrapServers());
        kafkaProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, conf.getNotificationClientId());
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, conf.getNotificationGroupId());
        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "30000");  // reduce this value to 30 seconds for the scenario where we subscribe before the topic exists.
        return kafkaProps;
    }

    private static Properties getKafkaProducerProperties(final PeriodicNotificationApplicationConfiguration conf) {
        final Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getBootStrapServers());
        return kafkaProps;
    }
}
