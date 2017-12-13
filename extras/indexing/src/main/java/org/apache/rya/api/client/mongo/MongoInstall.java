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

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
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
import org.apache.rya.api.layout.TablePrefixLayoutStrategy;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;

import com.google.common.base.Optional;
import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Mongo implementation of the {@link Install} command.
 */
@DefaultAnnotation(NonNull.class)
public class MongoInstall extends MongoCommand implements Install {

    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link MongoInstall}.
     *
     * @param connectionDetails
     *            - Details about the values that were used to create the connector to the cluster. (not null)
     * @param client
     *            - Provides programatic access to the instance of Mongo
     *            that hosts Rya instance. (not null)
     */
    public MongoInstall(final MongoConnectionDetails connectionDetails, final MongoClient client) {
        super(connectionDetails, client);
        instanceExists = new MongoInstanceExists(connectionDetails, client);
    }

    @Override
    public void install(final String instanceName, final InstallConfiguration installConfig) throws DuplicateInstanceNameException, RyaClientException {
        requireNonNull(instanceName, "instanceName required.");
        requireNonNull(installConfig, "installConfig required.");

        // Check to see if a Rya instance has already been installed with this name.
        if (instanceExists.exists(instanceName)) {
            throw new DuplicateInstanceNameException("An instance of Rya has already been installed to this Rya storage " + "with the name '" + instanceName + "'. Try again with a different name.");
        }

        // Initialize the Rya Details table.
        RyaDetails details;
        try {
            details = initializeRyaDetails(instanceName, installConfig);
        } catch (final AlreadyInitializedException e) {
            // This can only happen if somebody else installs an instance of Rya with the name between the check and now.
            throw new DuplicateInstanceNameException("An instance of Rya has already been installed to this Rya storage "//
                            + "with the name '" + instanceName//
                            + "'. Try again with a different name.");
        } catch (final RyaDetailsRepositoryException e) {
            throw new RyaClientException("The RyaDetails couldn't be initialized. Details: " + e.getMessage(), e);
        }

        // Initialize the rest of the tables used by the Rya instance.
        final MongoDBRdfConfiguration ryaConfig = makeRyaConfig(getMongoConnectionDetails(), details);
        try {
            final MongoDBRyaDAO ryaDao = new MongoDBRyaDAO(ryaConfig, getClient());
            ryaDao.setConf(ryaConfig);

            final TablePrefixLayoutStrategy tls = new TablePrefixLayoutStrategy();
            tls.setTablePrefix(instanceName);
            ryaConfig.setTableLayoutStrategy(tls);

            ryaDao.init();
        } catch (final RyaDAOException e) {
            throw new RyaClientException("Could not initialize all of the tables for the new Rya instance. " //
                            + "This instance may be left in a bad state.", e);
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
     * @param instanceName
     *            - The name of the instance that is being created. (not null)
     * @param installConfig
     *            - The instance's install configuration. (not null)
     * @return The {@link RyaDetails} that were stored.
     * @throws AlreadyInitializedException
     *             Could not be initialized because
     *             a table with this instance name has already exists and is holding the details.
     * @throws RyaDetailsRepositoryException
     *             Something caused the initialization
     *             operation to fail.
     */
    private RyaDetails initializeRyaDetails(final String instanceName, final InstallConfiguration installConfig) throws AlreadyInitializedException, RyaDetailsRepositoryException {
        final RyaDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(getClient(), instanceName);

        // Build the PCJ Index details. [not supported in mongo]
        final PCJIndexDetails.Builder pcjDetailsBuilder = PCJIndexDetails.builder().setEnabled(false);

        final RyaDetails details = RyaDetails.builder()
                        // General Metadata
                        .setRyaInstanceName(instanceName).setRyaVersion(getVersion())

                        // Secondary Index Values
                        // FIXME .setGeoIndexDetails(new GeoIndexDetails(installConfig.isGeoIndexEnabled()))
                        .setTemporalIndexDetails(new TemporalIndexDetails(installConfig.isTemporalIndexEnabled())) //
                        .setFreeTextDetails(new FreeTextIndexDetails(installConfig.isFreeTextIndexEnabled()))//
                        .setEntityCentricIndexDetails(new EntityCentricIndexDetails(false))// not supported in mongo
                        .setPCJIndexDetails(pcjDetailsBuilder)

                        // Statistics values.
                        .setProspectorDetails(new ProspectorDetails(Optional.<Date> absent()))//
                        .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date> absent()))//
                        .build();

        // Initialize the table.
        detailsRepo.initialize(details);

        return details;
    }

    /**
     * Builds a {@link MongoRdfConfiguration} object that will be used by the
     * Rya DAO to initialize all of the tables it will need.
     *
     * @param connectionDetails
     *            - Indicates how to connect to Mongo. (not null)
     * @param details
     *            - Indicates what needs to be installed. (not null)
     * @return A Rya Configuration object that can be used to perform the install.
     */
    private static MongoDBRdfConfiguration makeRyaConfig(final MongoConnectionDetails connectionDetails, final RyaDetails details) {
        final MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration();
        conf.setBoolean(ConfigUtils.USE_MONGO, true);
        // The Rya Instance Name is used as a prefix for the index tables in Mongo.
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, details.getRyaInstanceName());

        // Enable the indexers that the instance is configured to use.
        conf.set(ConfigUtils.USE_PCJ, "" + details.getPCJIndexDetails().isEnabled());
        // fixme conf.set(OptionalConfigUtils.USE_GEO, "" + details.getGeoIndexDetails().isEnabled() );
        conf.set(ConfigUtils.USE_FREETEXT, "" + details.getFreeTextIndexDetails().isEnabled());
        conf.set(ConfigUtils.USE_TEMPORAL, "" + details.getTemporalIndexDetails().isEnabled());

        // Mongo does not support entity indexing.
        conf.set(ConfigUtils.USE_ENTITY, "" + false);

        conf.set(ConfigUtils.CLOUDBASE_USER, connectionDetails.getUsername());
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, new String(connectionDetails.getPassword()));
        conf.set(MongoDBRdfConfiguration.MONGO_DB_NAME, details.getRyaInstanceName());

        // This initializes the living indexers that will be used by the application and
        // caches them within the configuration object so that they may be used later.
        ConfigUtils.setIndexers(conf);

        return conf;
    }
}