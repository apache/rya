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

import java.net.UnknownHostException;
import java.util.Objects;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.conf.Configuration;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
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
import org.apache.rya.sail.config.RyaSailFactory;

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
            final MongoDBRdfConfiguration mongoConfig = new MongoDBRdfConfiguration(config);
            rdfConfig = mongoConfig;
            final MongoClient client = MongoConnectorFactory.getMongoClient(config);
            try {
                final MongoRyaInstanceDetailsRepository ryaDetailsRepo = new MongoRyaInstanceDetailsRepository(client, mongoConfig.getCollectionName());
                RyaDetailsToConfiguration.addRyaDetailsToConfiguration(ryaDetailsRepo.getRyaInstanceDetails(), mongoConfig);
            } catch (final RyaDetailsRepositoryException e) {
               LOG.info("Instance does not have a rya details collection, skipping.");
           }            dao = getMongoDAO((MongoDBRdfConfiguration)rdfConfig, client);
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

    private static MongoDBRyaDAO getMongoDAO(final MongoDBRdfConfiguration config, final MongoClient client) throws RyaDAOException {
        MongoDBRyaDAO dao = null;
        OptionalConfigUtils.setIndexers(config);
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