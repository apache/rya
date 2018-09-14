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
package org.apache.rya.indexing;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsToConfiguration;
import org.apache.rya.api.layout.TablePrefixLayoutStrategy;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.OptionalConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.MongoSecondaryIndex;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.apache.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

public class GeoRyaSailFactory {
    private static final Logger LOG = LoggerFactory.getLogger(GeoRyaSailFactory.class);

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

            // Create the MongoClient that will be used by the Sail object's components.
            final MongoClient client = createMongoClient(mongoConfig);

            // Add the Indexer and Optimizer names to the configuration object that are configured to be used.
            OptionalConfigUtils.setIndexers(mongoConfig);

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
            rdfConfig = statefulConfig;

            // Create the DAO that is able to interact with MongoDB.
            final MongoDBRyaDAO mongoDao = new MongoDBRyaDAO();
            mongoDao.setConf(statefulConfig);
            mongoDao.init();
            dao = mongoDao;
        } else {
            rdfConfig = new AccumuloRdfConfiguration(config);
            user = rdfConfig.get(ConfigUtils.CLOUDBASE_USER);
            pswd = rdfConfig.get(ConfigUtils.CLOUDBASE_PASSWORD);
            Objects.requireNonNull(user, "Accumulo user name is missing from configuration."+ConfigUtils.CLOUDBASE_USER);
            Objects.requireNonNull(pswd, "Accumulo user password is missing from configuration."+ConfigUtils.CLOUDBASE_PASSWORD);
            rdfConfig.setTableLayoutStrategy( new TablePrefixLayoutStrategy(ryaInstance) );
            RyaSailFactory.updateAccumuloConfig((AccumuloRdfConfiguration) rdfConfig, user, pswd, ryaInstance);
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
        requireNonNull(mongoConf.getRyaInstanceName());

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
        final String database = mongoConf.getRyaInstanceName();
        final String password = mongoConf.getMongoPassword();
        if(username != null && password != null) {
            final MongoCredential cred = MongoCredential.createCredential(username, database, password.toCharArray());
            return new MongoClient(server, Arrays.asList(cred));
        } else {
            return new MongoClient(server);
        }
    }

    private static AccumuloRyaDAO getAccumuloDAO(final AccumuloRdfConfiguration config) throws AccumuloException, AccumuloSecurityException, RyaDAOException {
        final Connector connector = ConfigUtils.getConnector(config);
        final AccumuloRyaDAO dao = new AccumuloRyaDAO();
        dao.setConnector(connector);

        OptionalConfigUtils.setIndexers(config);
        config.setDisplayQueryPlan(true);

        dao.setConf(config);
        dao.init();
        return dao;
    }
}