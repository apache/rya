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

import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.export.Accumulo;
import org.apache.rya.export.Connection;
import org.apache.rya.export.MergeToolConfiguration;
import org.apache.rya.export.Mongo;
import org.apache.rya.export.accumulo.AccumuloRyaStatementStore;
import org.apache.rya.export.accumulo.conf.InstanceType;
import org.apache.rya.export.accumulo.policy.TimestampPolicyAccumuloRyaStatementStore;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.conf.MergeConfigHadoopAdapter;
import org.apache.rya.export.client.conf.MergeConfigurationException;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.export.mongo.policy.TimestampPolicyMongoRyaStatementStore;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;

import com.mongodb.MongoClient;

/**
 * Factory for creating {@link RyaStatementStore}s based on the {@link MergeConfiguration}.
 */
public class StatementStoreFactory {
    private final MergeToolConfiguration configuration;

    public StatementStoreFactory(final MergeToolConfiguration configuration) {
        this.configuration = requireNonNull(configuration);
    }

    /**
     * Builds and retrieves the Parent {@link RyaStatementStore}.
     * @return The created {@link RyaStatementStore} that connects to the Parent rya.
     * @throws Exception - Something went wrong creating the {@link RyaStatementStore}.
     */
    public RyaStatementStore getStatementStore(final Connection connection) throws Exception {
        final long timestamp = configuration.getStartTime();
        final String ryaInstanceName = connection.getRyaInstanceName();

        if(connection.getAccumulo() != null) {
            final Accumulo accumulo = connection.getAccumulo();
            final AccumuloRyaStatementStore store = getAccumuloStore(accumulo, ryaInstanceName);
            return new TimestampPolicyAccumuloRyaStatementStore(store, timestamp);
        } else if(connection.getMongo() != null) {
            final Mongo mongo = connection.getMongo();
            final MongoRyaStatementStore store = getMongoStore(mongo, connection.getRyaInstanceName());
            return new TimestampPolicyMongoRyaStatementStore(store, timestamp);
        } else {
            throw new MergeConfigurationException("No parent database was specified.");
        }
    }

    private MongoRyaStatementStore getMongoStore(final Mongo mongo, final String ryaInstanceName) throws RyaDAOException {
        final MongoClient client = new MongoClient(mongo.getHostname(), mongo.getPort());
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        dao.setConf(new StatefulMongoDBRdfConfiguration(MergeConfigHadoopAdapter.getMongoConfiguration(mongo, ryaInstanceName), client));
        dao.init();
        return new MongoRyaStatementStore(client, ryaInstanceName, dao);
    }

    private AccumuloRyaStatementStore getAccumuloStore(final Accumulo accumulo, final String ryaInstanceName) throws Exception {
        final AccumuloInstanceDriver aInstance = new AccumuloInstanceDriver(
                ryaInstanceName+"_driver",
                InstanceType.valueOf(accumulo.getInstanceType().toString()),
                true, false, accumulo.equals(configuration.getParent().getAccumulo()),
                accumulo.getUsername(),
                accumulo.getPassword(),
                ryaInstanceName,
                accumulo.getTablePrefix(),
                accumulo.getAuths(),
                accumulo.getZookeepers());
        aInstance.setUp();
        final AccumuloRyaDAO dao = aInstance.getDao();
        return new AccumuloRyaStatementStore(dao, accumulo.getTablePrefix(), ryaInstanceName);
    }
}
