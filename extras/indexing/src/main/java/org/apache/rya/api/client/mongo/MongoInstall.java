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

import java.util.Date;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.AlreadyInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Mongo implementation of the {@link Install} command.
 * </p>
 * Notes about the scheme:
 * <ul>
 *   <li>The Rya instance name is used as the DB name in Mongo.</li>
 *   <li>The Rya triples, instance details, and Rya indexes each get their own collection.</li>
 *   <li>Each Mongo DB can have only one Rya instance.</li>
 * </ul>
 */
@DefaultAnnotation(NonNull.class)
public class MongoInstall implements Install {

    private static final Logger log = LoggerFactory.getLogger(MongoInstall.class);

    private final MongoConnectionDetails connectionDetails;
    private final MongoClient adminClient;
    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link MongoInstall}.
     *
     * @param connectionDetails - Details to connect to the server. (not null)
     * @param adminClient - Provides programmatic access to the instance of Mongo that hosts Rya instances. (not null)
     * @param instanceExists - The interactor used to check if a Rya instance exists. (not null)
     */
    public MongoInstall(
            final MongoConnectionDetails connectionDetails,
            final MongoClient adminClient,
            final MongoInstanceExists instanceExists) {
        this.connectionDetails = requireNonNull(connectionDetails);
        this.adminClient = requireNonNull(adminClient);
        this.instanceExists = requireNonNull(instanceExists);
    }

    @Override
    public void install(final String instanceName, final InstallConfiguration installConfig) throws DuplicateInstanceNameException, RyaClientException {
        requireNonNull(instanceName);
        requireNonNull(installConfig);

        // Check to see if a Rya instance has already been installed with this name.
        if (instanceExists.exists(instanceName)) {
            throw new DuplicateInstanceNameException("An instance of Rya has already been installed to this Rya storage " + "with the name '" + instanceName + "'. Try again with a different name.");
        }

        // Initialize the Rya Details table.
        final RyaDetails ryaDetails;
        try {
            ryaDetails = initializeRyaDetails(instanceName, installConfig);
        } catch (final AlreadyInitializedException e) {
            // This can only happen if somebody else installs an instance of Rya with the name between the check and now.
            throw new DuplicateInstanceNameException(
                    "An instance of Rya has already been installed to this Rya storage " +
                            "with the name '" + instanceName + "'. Try again with a different name.");
        } catch (final RyaDetailsRepositoryException e) {
            throw new RyaClientException("The RyaDetails couldn't be initialized. Details: " + e.getMessage(), e);
        }

        // Initialize the rest of the collections used by the Rya instance.
        final MongoDBRdfConfiguration ryaConfig = makeRyaConfig(connectionDetails, ryaDetails);
        try {
            final Sail ryaSail = RyaSailFactory.getInstance(ryaConfig);
            ryaSail.shutDown();
        } catch (final AccumuloException | AccumuloSecurityException | RyaDAOException | InferenceEngineException e) {
            throw new RyaClientException("Could not initialize all of the tables for the new Rya instance. " +
                    "This instance may be left in a bad state.", e);
        } catch (final SailException e) {
            throw new RyaClientException("Problem shutting down the Sail object used to install Rya.", e);
        }
    }

    /**
     * @return The version of the application as reported by the manifest.
     */
    private String getVersion() {
        return "" + this.getClass().getPackage().getImplementationVersion();
    }

    /**
     * Initializes the {@link RyaDetails} and stores them for the new instance.
     *
     * @param instanceName - The name of the instance that is being created. (not null)
     * @param installConfig - The instance's install configuration. (not null)
     * @return The {@link RyaDetails} that were stored.
     * @throws AlreadyInitializedException Could not be initialized because a table with this instance name has already
     *   exists and is holding the details.
     * @throws RyaDetailsRepositoryException Something caused the initialization operation to fail.
     */
    private RyaDetails initializeRyaDetails(
            final String instanceName,
            final InstallConfiguration installConfig) throws AlreadyInitializedException, RyaDetailsRepositoryException {
        final RyaDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(adminClient, instanceName);

        if(installConfig.getFluoPcjAppName().isPresent()) {
        	log.warn("Mongo does not have fluo support, use ignoring the configured fluo application name: " + installConfig.getFluoPcjAppName().get());
        }
        
        // Build the PCJ Index details.
        final PCJIndexDetails.Builder pcjDetailsBuilder = PCJIndexDetails.builder()
                .setEnabled(installConfig.isPcjIndexEnabled());

        final RyaDetails details = RyaDetails.builder()
                // General Metadata
                .setRyaInstanceName(instanceName).setRyaVersion(getVersion())

                // Secondary Index Values
                // FIXME RYA-215 .setGeoIndexDetails(new GeoIndexDetails(installConfig.isGeoIndexEnabled()))
                .setTemporalIndexDetails(new TemporalIndexDetails(installConfig.isTemporalIndexEnabled()))
                .setFreeTextDetails(new FreeTextIndexDetails(installConfig.isFreeTextIndexEnabled()))
                .setEntityCentricIndexDetails(new EntityCentricIndexDetails(false))
                .setPCJIndexDetails(pcjDetailsBuilder)

                // Statistics values.
                .setProspectorDetails(new ProspectorDetails(Optional.<Date> absent()))
                .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date> absent()))
                .build();

        // Initialize the table.
        detailsRepo.initialize(details);

        return details;
    }

    /**
     * Builds a {@link MongoRdfConfiguration} object that will be used by the
     * Rya DAO to initialize all of the tables it will need.
     *
     * @param connectionDetails - Indicates how to connect to Mongo. (not null)
     * @param ryaDetails - Indicates what needs to be installed. (not null)
     * @return A Rya Configuration object that can be used to perform the install.
     */
    private static MongoDBRdfConfiguration makeRyaConfig(
            final MongoConnectionDetails connectionDetails,
            final RyaDetails ryaDetails) {
        // Start with a configuration that is built using the connection details.

        final MongoDBRdfConfiguration conf = connectionDetails.build(ryaDetails.getRyaInstanceName());

        conf.setBoolean(ConfigUtils.USE_PCJ, ryaDetails.getPCJIndexDetails().isEnabled());

        // Mongo does not support entity indexing.
        if(ryaDetails.getEntityCentricIndexDetails().isEnabled()) {
            log.warn("The install configuration says to enable Entity Centric indexing, but Mongo RYA does not support " +
                    "that feature. Ignoring this configuration.");
        }

        //TODO mongo now has an entity index, just needs CLI support.
        conf.setBoolean(ConfigUtils.USE_ENTITY, false);

        // FIXME RYA-215 We haven't enabled geo indexing in the console yet.
        //conf.set(OptionalConfigUtils.USE_GEO, "" + details.getGeoIndexDetails().isEnabled() );

        // Enable the supported indexers that the instance is configured to use.
        conf.setBoolean(ConfigUtils.USE_FREETEXT, ryaDetails.getFreeTextIndexDetails().isEnabled());
        conf.setBoolean(ConfigUtils.USE_TEMPORAL, ryaDetails.getTemporalIndexDetails().isEnabled());

        // This initializes the living indexers that will be used by the application and
        // caches them within the configuration object so that they may be used later.
        ConfigUtils.setIndexers(conf);

        return conf;
    }
}