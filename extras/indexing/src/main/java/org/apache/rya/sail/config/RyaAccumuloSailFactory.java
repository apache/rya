package org.apache.rya.sail.config;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.openrdf.sail.Sail;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailImplConfig;

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

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;

/**
 * @deprecated Use {@link RyaSailFactory} instead.
 */
@Deprecated
public class RyaAccumuloSailFactory implements SailFactory {

    public static final String SAIL_TYPE = "rya:RyaAccumuloSail";

    @Override
    public SailImplConfig getConfig() {
        return new RyaAccumuloSailConfig();
    }

    @Override
    public Sail getSail(final SailImplConfig config) throws SailConfigException {
        try {
            final RdfCloudTripleStore store = new RdfCloudTripleStore();
            final RyaAccumuloSailConfig cbconfig = (RyaAccumuloSailConfig) config;

            final String instanceName = cbconfig.getInstance();
            final String zooKeepers = cbconfig.getZookeepers();

            Instance i;
            if (cbconfig.isMock()) {
                i = new MockInstance(instanceName);
            } else {
                i = new ZooKeeperInstance(instanceName, zooKeepers);
            }

            final String user = cbconfig.getUser();
            final String pass = cbconfig.getPassword();

            final Connector connector = i.getConnector(user, new PasswordToken(pass));
            final AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
            crdfdao.setConnector(connector);

            final AccumuloRdfConfiguration conf = cbconfig.toRdfConfiguation();
            ConfigUtils.setIndexers(conf);
            conf.setDisplayQueryPlan(true);

            crdfdao.setConf(conf);
            crdfdao.init();
            store.setRyaDAO(crdfdao);

            return store;
        } catch (RyaDAOException | AccumuloException | AccumuloSecurityException e) {
            throw new SailConfigException(e);
        }
    }

    @Override
    public String getSailType() {
        return SAIL_TYPE;
    }

}
