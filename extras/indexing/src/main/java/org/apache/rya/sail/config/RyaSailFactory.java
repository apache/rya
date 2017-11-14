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
package org.apache.rya.sail.config;

import static java.util.Objects.requireNonNull;

import java.net.UnknownHostException;
import java.util.Objects;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
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
import org.apache.rya.mongodb.MongoConnectorFactory;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;

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
            final MongoDBRdfConfiguration mongoConfig = new MongoDBRdfConfiguration(config);
            rdfConfig = mongoConfig;
            final MongoClient client = MongoConnectorFactory.getMongoClient(config);
            try {
                final MongoRyaInstanceDetailsRepository ryaDetailsRepo = new MongoRyaInstanceDetailsRepository(client, mongoConfig.getCollectionName());
                RyaDetailsToConfiguration.addRyaDetailsToConfiguration(ryaDetailsRepo.getRyaInstanceDetails(), mongoConfig);
            } catch (final RyaDetailsRepositoryException e) {
               LOG.info("Instance does not have a rya details collection, skipping.");
           }
            dao = getMongoDAO((MongoDBRdfConfiguration)rdfConfig, client);
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

    private static MongoDBRyaDAO getMongoDAO(final MongoDBRdfConfiguration config, final MongoClient client) throws RyaDAOException {
        MongoDBRyaDAO dao = null;
        ConfigUtils.setIndexers(config);
        if(client != null) {
            dao = new MongoDBRyaDAO(config, client);
        } else {
            try {
                dao = new MongoDBRyaDAO(config);
            } catch (NumberFormatException | UnknownHostException e) {
                throw new RyaDAOException("Unable to connect to mongo at the configured location.", e);
            }
        }
        dao.init();
        return dao;
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
        
        String ryaInstance = config.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);
        Objects.requireNonNull(ryaInstance, "RyaInstance or table prefix is missing from configuration."+RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);
        String user = config.get(AccumuloRdfConfiguration.CLOUDBASE_USER);
        String pswd = config.get(AccumuloRdfConfiguration.CLOUDBASE_PASSWORD);
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
}