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
package org.apache.rya.export.client.merge;

import static java.util.Objects.requireNonNull;

import java.util.Date;

import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.export.DBType;
import org.apache.rya.export.InstanceType;
import org.apache.rya.export.MergePolicy;
import org.apache.rya.export.accumulo.AccumuloRyaStatementStore;
import org.apache.rya.export.accumulo.policy.TimestampPolicyAccumuloRyaStatementStore;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.apache.rya.export.api.conf.AccumuloMergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.export.api.conf.policy.TimestampPolicyMergeConfiguration;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.conf.MergeConfigHadoopAdapter;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.export.mongo.policy.TimestampPolicyMongoRyaStatementStore;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;

import com.mongodb.MongoClient;

/**
 * Factory for creating {@link RyaStatementStore}s based on the {@link MergeConfiguration}.
 */
public class StatementStoreFactory {
    private final MergeConfiguration configuration;

    public StatementStoreFactory(final MergeConfiguration configuration) {
        this.configuration = requireNonNull(configuration);
    }

    /**
     * Builds and retrieves the Parent {@link RyaStatementStore}.
     * @return The created {@link RyaStatementStore} that connects to the Parent rya.
     * @throws Exception - Something went wrong creating the {@link RyaStatementStore}.
     */
    public RyaStatementStore getParentStatementStore() throws Exception {
        final DBType dbType = configuration.getParentDBType();
        final String ryaInstanceName = configuration.getParentRyaInstanceName();
        RyaStatementStore store = getBaseStatementStore(dbType,
                configuration.getParentHostname(),
                configuration.getParentPort(),
                ryaInstanceName,
                configuration.getParentTablePrefix(),
                configuration, true);
        store = getMergePolicyStatementStore(store,
                configuration.getMergePolicy(),
                ryaInstanceName,
                dbType);
        return store;
    }

    public RyaStatementStore getChildStatementStore() throws Exception {
        final RyaStatementStore store = getBaseStatementStore(configuration.getChildDBType(),
                configuration.getChildHostname(),
                configuration.getChildPort(),
                configuration.getChildRyaInstanceName(),
                configuration.getChildTablePrefix(),
                configuration, false);
        return store;
    }

    /**
     * @param isParent
     * @param config
     *          These parameters are hacks until the Accumulo DAO only accepts a connector.
     *          Once that happens this will be much, much cleaner, and make the {@link AccumuloInstanceDriver}
     *          obsolete.
     * @throws Exception
     */
    private RyaStatementStore getBaseStatementStore(final DBType dbType,
            final String hostname, final int port, final String ryaInstancename,
            final String tablePrefix, final MergeConfiguration config, final boolean isParent) throws Exception {
        RyaStatementStore store;
        if(dbType == DBType.MONGO) {
            store = getBaseMongoStore(hostname, port, ryaInstancename);
        } else {
            final AccumuloMergeConfiguration aConfig = (AccumuloMergeConfiguration) config;
            final InstanceType type = isParent ? aConfig.getParentInstanceType() : aConfig.getChildInstanceType();
            store = getBaseAccumuloStore(ryaInstancename, type, isParent, ryaInstancename, tablePrefix, tablePrefix, tablePrefix, tablePrefix);
        }
        return store;
    }

    private RyaStatementStore getMergePolicyStatementStore(final RyaStatementStore store, final MergePolicy policy, final String ryaInstanceName, final DBType dbType) {
        RyaStatementStore policyStore = null;
        if(policy == MergePolicy.TIMESTAMP) {
            final TimestampPolicyMergeConfiguration timeConfig = (TimestampPolicyMergeConfiguration) configuration;
            final Date timestamp = timeConfig.getToolStartTime();
            if(dbType == DBType.MONGO) {
                policyStore = new TimestampPolicyMongoRyaStatementStore((MongoRyaStatementStore) store, timestamp, ryaInstanceName);
            } else {
                policyStore = new TimestampPolicyAccumuloRyaStatementStore((AccumuloRyaStatementStore) store, timestamp);
            }
        }
        return policyStore == null ? store : policyStore;
    }

    private MongoRyaStatementStore getBaseMongoStore(final String hostname, final int port, final String ryaInstanceName) throws RyaDAOException {
        final MongoClient client = new MongoClient(hostname, port);
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        dao.setConf(new StatefulMongoDBRdfConfiguration(MergeConfigHadoopAdapter.getMongoConfiguration(configuration), client));
        dao.init();
        return new MongoRyaStatementStore(client, ryaInstanceName, dao);
    }

    private AccumuloRyaStatementStore getBaseAccumuloStore(final String ryaInstanceName,
            final InstanceType type, final boolean isParent,
            final String username, final String password, final String tablePrefix,
            final String auths, final String zookeepers) throws Exception {
        final AccumuloInstanceDriver aInstance = new AccumuloInstanceDriver(
            ryaInstanceName+"_driver", type, true, false, isParent, username,
            password, ryaInstanceName, tablePrefix, auths, zookeepers);
        aInstance.setUp();
        final AccumuloRyaDAO dao = aInstance.getDao();
        return new AccumuloRyaStatementStore(dao, tablePrefix, ryaInstanceName);
    }
}
