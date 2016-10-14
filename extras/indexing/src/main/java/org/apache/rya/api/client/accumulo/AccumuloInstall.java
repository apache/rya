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

import java.util.Date;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

import com.google.common.base.Optional;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.GeoIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.AlreadyInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;

/**
 * An Accumulo implementation of the {@link Install} command.
 */

@ParametersAreNonnullByDefault
public class AccumuloInstall extends AccumuloCommand implements Install {

    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link AccumuloInstall}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloInstall(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        instanceExists = new AccumuloInstanceExists(connectionDetails, connector);
    }

    @Override
    public void install(final String instanceName, final InstallConfiguration installConfig) throws DuplicateInstanceNameException, RyaClientException {
        requireNonNull(instanceName);
        requireNonNull(installConfig);

        // Check to see if a Rya instance has already been installed with this name.
        if(instanceExists.exists(instanceName)) {
            throw new DuplicateInstanceNameException("An instance of Rya has already been installed to this Rya storage " +
                    "with the name '" + instanceName + "'. Try again with a different name.");
        }

        // Initialize the Rya Details table.
        RyaDetails details;
        try {
            details = initializeRyaDetails(instanceName, installConfig);
        } catch (final AlreadyInitializedException e) {
            // This can only happen if somebody else installs an instance of Rya with the name between the check and now.
            throw new DuplicateInstanceNameException("An instance of Rya has already been installed to this Rya storage " +
                    "with the name '" + instanceName + "'. Try again with a different name.");
        } catch (final RyaDetailsRepositoryException e) {
            throw new RyaClientException("The RyaDetails couldn't be initialized. Details: " + e.getMessage(), e);
        }

        // Initialize the rest of the tables used by the Rya instance.
        final AccumuloRdfConfiguration ryaConfig = makeRyaConfig(getAccumuloConnectionDetails(), details);
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
     * @throws AlreadyInitializedException Could not be initialized because
     *   a table with this instance name has already exists and is holding the details.
     * @throws RyaDetailsRepositoryException Something caused the initialization
     *   operation to fail.
     */
    private RyaDetails initializeRyaDetails(final String instanceName, final InstallConfiguration installConfig)
            throws AlreadyInitializedException, RyaDetailsRepositoryException {
        final RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(getConnector(), instanceName);

        // Build the PCJ Index details.
        final PCJIndexDetails.Builder pcjDetailsBuilder = PCJIndexDetails.builder()
                .setEnabled(installConfig.isPcjIndexEnabled());
        if(installConfig.getFluoPcjAppName().isPresent()) {
            final String fluoPcjAppName = installConfig.getFluoPcjAppName().get();
            pcjDetailsBuilder.setFluoDetails(new FluoDetails( fluoPcjAppName ));
        }

        final RyaDetails details = RyaDetails.builder()
                // General Metadata
                .setRyaInstanceName(instanceName)
                .setRyaVersion( getVersion() )

                // Secondary Index Values
                .setGeoIndexDetails(
                        new GeoIndexDetails(installConfig.isGeoIndexEnabled()))
                .setTemporalIndexDetails(
                        new TemporalIndexDetails(installConfig.isTemporalIndexEnabled()))
                .setFreeTextDetails(
                        new FreeTextIndexDetails(installConfig.isFreeTextIndexEnabled()))
                .setEntityCentricIndexDetails(
                        new EntityCentricIndexDetails(installConfig.isEntityCentrixIndexEnabled()))
                .setPCJIndexDetails(  pcjDetailsBuilder )

                // Statistics values.
                .setProspectorDetails(
                        new ProspectorDetails(Optional.<Date>absent()) )
                .setJoinSelectivityDetails(
                        new JoinSelectivityDetails(Optional.<Date>absent()) )
                .build();

        // Initialize the table.
        detailsRepo.initialize(details);

        return details;
    }

    /**
     * Builds a {@link AccumuloRdfConfiguration} object that will be used by the
     * Rya DAO to initialize all of the tables it will need.
     *
     * @param connectionDetails - Indicates how to connect to Accumulo. (not null)
     * @param details - Indicates what needs to be installed. (not null)
     * @return A Rya Configuration object that can be used to perform the install.
     */
    private static AccumuloRdfConfiguration makeRyaConfig(final AccumuloConnectionDetails connectionDetails, final RyaDetails details) {
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        // The Rya Instance Name is used as a prefix for the index tables in Accumulo.
        conf.setTablePrefix( details.getRyaInstanceName() );

        // Enable the indexers that the instance is configured to use.
        // TODO fix me, not sure why the install command is here.
//        conf.set(ConfigUtils.USE_GEO, "" + details.getGeoIndexDetails().isEnabled() );
        conf.set(ConfigUtils.USE_FREETEXT, "" + details.getFreeTextIndexDetails().isEnabled() );
        conf.set(ConfigUtils.USE_TEMPORAL, "" + details.getTemporalIndexDetails().isEnabled() );
        conf.set(ConfigUtils.USE_ENTITY, "" + details.getEntityCentricIndexDetails().isEnabled());

        conf.set(ConfigUtils.USE_PCJ, "" + details.getPCJIndexDetails().isEnabled() );
        conf.set(ConfigUtils.PCJ_STORAGE_TYPE, PrecomputedJoinStorageType.ACCUMULO.toString());

        final Optional<FluoDetails> fluoHolder = details.getPCJIndexDetails().getFluoDetails();
        final PrecomputedJoinUpdaterType updaterType = fluoHolder.isPresent() ? PrecomputedJoinUpdaterType.FLUO : PrecomputedJoinUpdaterType.NO_UPDATE;
        conf.set(ConfigUtils.PCJ_UPDATER_TYPE, updaterType.toString());

        // XXX The Accumulo implementation of the secondary indices make need all
        //     of the accumulo connector's parameters to initialize themselves, so
        //     we need to include them here. It would be nice if the secondary
        //     indexers used the connector that is provided to them instead of
        //     building a new one.
        conf.set(ConfigUtils.CLOUDBASE_USER, connectionDetails.getUsername());
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, new String(connectionDetails.getPassword()));
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, connectionDetails.getInstanceName());
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, connectionDetails.getZookeepers());

        // This initializes the living indexers that will be used by the application and
        // caches them within the configuration object so that they may be used later.
        ConfigUtils.setIndexers(conf);

        return conf;
    }
}