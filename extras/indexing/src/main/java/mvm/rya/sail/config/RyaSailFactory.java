package mvm.rya.sail.config;

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

import java.net.UnknownHostException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.openrdf.sail.Sail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import mvm.rya.api.instance.RyaDetailsToConfiguration;
import mvm.rya.api.persist.RyaDAO;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.mongodb.MongoDBRdfConfiguration;
import mvm.rya.mongodb.MongoDBRyaDAO;
import mvm.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;
import mvm.rya.rdftriplestore.inference.InferenceEngine;
import mvm.rya.rdftriplestore.inference.InferenceEngineException;

public class RyaSailFactory {
    private static final Logger LOG = LoggerFactory.getLogger(RyaSailFactory.class);

    public static Sail getInstance(final Configuration conf) throws AccumuloException,
        AccumuloSecurityException, RyaDAOException, InferenceEngineException {
        return getRyaSail(conf);
    }

    private static Sail getRyaSail(final Configuration config) throws InferenceEngineException, RyaDAOException, AccumuloException, AccumuloSecurityException {
        final RdfCloudTripleStore store = new RdfCloudTripleStore();
        final RyaDAO dao;
        final RdfCloudTripleStoreConfiguration rdfConfig;

        final String user = config.get(ConfigUtils.CLOUDBASE_USER);
        final String pswd = config.get(ConfigUtils.CLOUDBASE_PASSWORD);
        final String instance = config.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);
        if(ConfigUtils.getUseMongo(config)) {
            rdfConfig = new MongoDBRdfConfiguration(config);
            final MongoClient client = updateMongoConfig((MongoDBRdfConfiguration) rdfConfig, user, pswd, instance);
            dao = getMongoDAO((MongoDBRdfConfiguration)rdfConfig, client);
        } else {
            rdfConfig = new AccumuloRdfConfiguration(config);
            updateAccumuloConfig((AccumuloRdfConfiguration) rdfConfig, user, pswd, instance);
            dao = getAccumuloDAO((AccumuloRdfConfiguration)rdfConfig);
        }
        rdfConfig.setTablePrefix(instance);

        if (rdfConfig.isInfer()){
            final InferenceEngine inferenceEngine = new InferenceEngine();
            inferenceEngine.setConf(rdfConfig);
            inferenceEngine.setRyaDAO(dao);
            inferenceEngine.init();
            store.setInferenceEngine(inferenceEngine);
        }

        return store;
    }

    private static RyaDAO getMongoDAO(final MongoDBRdfConfiguration config, final MongoClient client) throws RyaDAOException {
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

    private static RyaDAO getAccumuloDAO(final AccumuloRdfConfiguration config) throws AccumuloException, AccumuloSecurityException, RyaDAOException {
        final Connector connector = ConfigUtils.getConnector(config);
        final AccumuloRyaDAO dao = new AccumuloRyaDAO();
        dao.setConnector(connector);

        ConfigUtils.setIndexers(config);
        config.setDisplayQueryPlan(true);

        dao.setConf(config);
        dao.init();
        return dao;
    }

    private static MongoClient updateMongoConfig(final MongoDBRdfConfiguration config, final String user, final String pswd, final String instance) throws RyaDAOException {
        final MongoCredential creds = MongoCredential.createCredential(user, instance, pswd.toCharArray());
        final String hostname = config.getMongoInstance();
        final int port = Integer.parseInt(config.getMongoPort());

        MongoClient client = null;
        try {
            client = new MongoClient(new ServerAddress(hostname, port), Lists.newArrayList(creds));
            final MongoRyaInstanceDetailsRepository ryaDetailsRepo = new MongoRyaInstanceDetailsRepository(client, config.getCollectionName());
            RyaDetailsToConfiguration.addRyaDetailsToConfiguration(ryaDetailsRepo.getRyaInstanceDetails(), config);
        } catch(final RyaDetailsRepositoryException e) {
            LOG.info("Instance does not have a rya details collection, skipping.", e);
        } catch (final UnknownHostException ue) {
            throw new RyaDAOException("Unable to connect to mongo at the configured location.", ue);
        }
        return client;
    }

    private static void updateAccumuloConfig(final AccumuloRdfConfiguration config, final String user, final String pswd, final String instance) throws AccumuloException, AccumuloSecurityException {
        try {
            final PasswordToken pswdToken = new PasswordToken(pswd);
            final Instance accInst = ConfigUtils.getInstance(config);
            final AccumuloRyaInstanceDetailsRepository ryaDetailsRepo = new AccumuloRyaInstanceDetailsRepository(accInst.getConnector(user, pswdToken), instance);
            RyaDetailsToConfiguration.addRyaDetailsToConfiguration(ryaDetailsRepo.getRyaInstanceDetails(), config);
        } catch(final RyaDetailsRepositoryException e) {
            LOG.info("Instance does not have a rya details collection, skipping.", e);
        }
    }
}