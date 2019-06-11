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
package org.apache.rya.sail.config;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsToConfiguration;
import org.apache.rya.api.layout.TablePrefixLayoutStrategy;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.MongoSecondaryIndex;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.apache.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

public class RyaSailFactory {
    private static final Logger LOG = LoggerFactory.getLogger(RyaSailFactory.class);

    /**
     * Creates an instance of {@link Sail} that is attached to a Rya instance.
     *
     * @param conf - Configures how the Sail object will be constructed. (not null)
     * @return A {@link Sail} object that is backed by a Rya datastore.
     * @throws SailException The object could not be created.
     */
    public static Sail getInstance(final Configuration conf) throws AccumuloException,
        AccumuloSecurityException, RyaDAOException, InferenceEngineException, SailException {
        requireNonNull(conf);
        return getRyaSail(conf);
    }

    private static Sail getRyaSail(final Configuration config) throws InferenceEngineException, RyaDAOException, AccumuloException, AccumuloSecurityException, SailException {
        final RdfCloudTripleStore store = new RdfCloudTripleStore();
        final RyaDAO<?> dao;
        final RdfCloudTripleStoreConfiguration rdfConfig;

        final String user;
        final String pswd;
        // XXX Should(?) be MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX inside the if below. RYA-135
        final String ryaInstance = config.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);
        Objects.requireNonNull(ryaInstance, "RyaInstance or table prefix is missing from configuration."+RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);

        if(ConfigUtils.getUseMongo(config)) {
            // Get a reference to a Mongo DB configuration object.
            final MongoDBRdfConfiguration mongoConfig = (config instanceof MongoDBRdfConfiguration) ?
                    (MongoDBRdfConfiguration)config : new MongoDBRdfConfiguration(config);
            // Instantiate a Mongo client and Mongo DAO.
            dao = getMongoDAO(mongoConfig);
            // Then use the DAO's newly-created stateful conf in place of the original
            rdfConfig = dao.getConf();
        } else {
            rdfConfig = new AccumuloRdfConfiguration(config);
            user = rdfConfig.get(ConfigUtils.CLOUDBASE_USER);
            pswd = rdfConfig.get(ConfigUtils.CLOUDBASE_PASSWORD);
            Objects.requireNonNull(user, "Accumulo user name is missing from configuration."+ConfigUtils.CLOUDBASE_USER);
            Objects.requireNonNull(pswd, "Accumulo user password is missing from configuration."+ConfigUtils.CLOUDBASE_PASSWORD);
            rdfConfig.setTableLayoutStrategy( new TablePrefixLayoutStrategy(ryaInstance) );
            updateAccumuloConfig((AccumuloRdfConfiguration) rdfConfig, user, pswd, ryaInstance);
            dao = getAccumuloDAO((AccumuloRdfConfiguration)rdfConfig);
        }
        store.setRyaDAO(dao);
        rdfConfig.setTablePrefix(ryaInstance);

        if (rdfConfig.isInfer()){
            final InferenceEngine inferenceEngine = new InferenceEngine();
            inferenceEngine.setConf(rdfConfig);
            inferenceEngine.setRyaDAO(dao);
            inferenceEngine.init();
            store.setInferenceEngine(inferenceEngine);
        }

        store.initialize();

        return store;
    }

    /**
     * Create a {@link MongoClient} that is connected to the configured database.
     *
     * @param mongoConf - Configures what will be connected to. (not null)
     * @throws ConfigurationRuntimeException An invalid port was provided by {@code mongoConf}.
     * @throws MongoException Couldn't connect to the MongoDB database.
     */
    private static MongoClient createMongoClient(final MongoDBRdfConfiguration mongoConf) throws ConfigurationRuntimeException, MongoException {
        requireNonNull(mongoConf);
        requireNonNull(mongoConf.getMongoHostname());
        requireNonNull(mongoConf.getMongoPort());
        requireNonNull(mongoConf.getMongoDBName());

        // Connect to a running MongoDB server.
        final int port;
        try {
            port = Integer.parseInt( mongoConf.getMongoPort() );
        } catch(final NumberFormatException e) {
            throw new ConfigurationRuntimeException("Port '" + mongoConf.getMongoPort() + "' must be an integer.");
        }

        final ServerAddress server = new ServerAddress(mongoConf.getMongoHostname(), port);

        // Connect to a specific MongoDB Database if that information is provided.
        final String username = mongoConf.getMongoUser();
        final String database = mongoConf.getMongoDBName();
        final String password = mongoConf.getMongoPassword();
        if(username != null && password != null) {
            final MongoCredential cred = MongoCredential.createCredential(username, database, password.toCharArray());
            final MongoClientOptions options = new MongoClientOptions.Builder().build();
            return new MongoClient(server, cred, options);
        } else {
            return new MongoClient(server);
        }
    }

    /**
     * Creates AccumuloRyaDAO without updating the AccumuloRdfConfiguration.  This method does not force
     * the user's configuration to be consistent with the Rya Instance configuration.  As a result, new index
     * tables might be created when using this method.  This method does not require the {@link AccumuloRyaInstanceDetailsRepository}
     * to exist.  This is for internal use, backwards compatibility and testing purposes only.  It is recommended that
     * {@link RyaSailFactory#getAccumuloDAOWithUpdatedConfig(AccumuloRdfConfiguration)} be used for new installations of Rya.
     *
     * @param config - user configuration
     * @return - AccumuloRyaDAO with Indexers configured according to user's specification
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws RyaDAOException
     */
    public static AccumuloRyaDAO getAccumuloDAO(final AccumuloRdfConfiguration config) throws AccumuloException, AccumuloSecurityException, RyaDAOException {
        final Connector connector = ConfigUtils.getConnector(config);
        final AccumuloRyaDAO dao = new AccumuloRyaDAO();
        dao.setConnector(connector);

        ConfigUtils.setIndexers(config);

        dao.setConf(config);
        dao.init();
        return dao;
    }

    /**
     * Creates an AccumuloRyaDAO after updating the AccumuloRdfConfiguration so that it is consistent
     * with the configuration of the RyaInstance that the user is trying to connect to.  This ensures
     * that user configuration aligns with Rya instance configuration and prevents the creation of
     * new index tables based on a user's query configuration.  This method requires the {@link AccumuloRyaInstanceDetailsRepository}
     * to exist.
     *
     * @param config - user's query configuration
     * @return - AccumuloRyaDAO with an updated configuration that is consistent with the Rya instance configuration
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws RyaDAOException
     */
    public static AccumuloRyaDAO getAccumuloDAOWithUpdatedConfig(final AccumuloRdfConfiguration config) throws AccumuloException, AccumuloSecurityException, RyaDAOException {

        final String ryaInstance = config.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);
        Objects.requireNonNull(ryaInstance, "RyaInstance or table prefix is missing from configuration."+RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);
        final String user = config.get(AccumuloRdfConfiguration.CLOUDBASE_USER);
        final String pswd = config.get(AccumuloRdfConfiguration.CLOUDBASE_PASSWORD);
        Objects.requireNonNull(user, "Accumulo user name is missing from configuration."+AccumuloRdfConfiguration.CLOUDBASE_USER);
        Objects.requireNonNull(pswd, "Accumulo user password is missing from configuration."+AccumuloRdfConfiguration.CLOUDBASE_PASSWORD);
        config.setTableLayoutStrategy( new TablePrefixLayoutStrategy(ryaInstance) );
        updateAccumuloConfig(config, user, pswd, ryaInstance);

        return getAccumuloDAO(config);
    }

    public static void updateAccumuloConfig(final AccumuloRdfConfiguration config, final String user, final String pswd, final String ryaInstance) throws AccumuloException, AccumuloSecurityException {
        try {
            final Connector connector = ConfigUtils.getConnector(config);
            final AccumuloRyaInstanceDetailsRepository ryaDetailsRepo = new AccumuloRyaInstanceDetailsRepository(connector, ryaInstance);
            RyaDetailsToConfiguration.addRyaDetailsToConfiguration(ryaDetailsRepo.getRyaInstanceDetails(), config);
        } catch(final RyaDetailsRepositoryException e) {
            LOG.info("Instance does not have a rya details collection, skipping.");
        }
    }

    /**
     * Connects to MongoDB and creates a MongoDBRyaDAO.
     * @param config - user configuration
     * @return - MongoDBRyaDAO with Indexers configured according to user's specification
     * @throws RyaDAOException if the DAO can't be initialized
     */
    public static MongoDBRyaDAO getMongoDAO(final MongoDBRdfConfiguration mongoConfig) throws RyaDAOException {
            // Create the MongoClient that will be used by the Sail object's components.
            final MongoClient client = createMongoClient(mongoConfig);

            // Add the Indexer and Optimizer names to the configuration object that are configured to be used.
            ConfigUtils.setIndexers(mongoConfig);

            // Populate the configuration using previously stored Rya Details if this instance uses them.
            try {
                final MongoRyaInstanceDetailsRepository ryaDetailsRepo = new MongoRyaInstanceDetailsRepository(client, mongoConfig.getRyaInstanceName());
                RyaDetailsToConfiguration.addRyaDetailsToConfiguration(ryaDetailsRepo.getRyaInstanceDetails(), mongoConfig);
            } catch (final RyaDetailsRepositoryException e) {
               LOG.info("Instance does not have a rya details collection, skipping.");
            }

            // Set the configuration to the stateful configuration that is used to pass the constructed objects around.
            final StatefulMongoDBRdfConfiguration statefulConfig = new StatefulMongoDBRdfConfiguration(mongoConfig, client);
            final List<MongoSecondaryIndex> indexers = statefulConfig.getInstances(AccumuloRdfConfiguration.CONF_ADDITIONAL_INDEXERS, MongoSecondaryIndex.class);
            statefulConfig.setIndexers(indexers);

            // Create the DAO that is able to interact with MongoDB.
            final MongoDBRyaDAO mongoDao = new MongoDBRyaDAO();
            mongoDao.setConf(statefulConfig);
            mongoDao.init();
            return mongoDao;
    }
}