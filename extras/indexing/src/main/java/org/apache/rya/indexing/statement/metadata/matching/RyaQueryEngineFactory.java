package org.apache.rya.indexing.statement.metadata.matching;
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

import com.mongodb.MongoClient;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.query.AccumuloRyaQueryEngine;
import org.apache.rya.api.RdfTripleStoreConfiguration;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.mongodb.MongoConnectorFactory;
import org.apache.rya.mongodb.MongoDBQueryEngine;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;

/**
 * THis class creates the appropriate {@link RyaQueryEngine} based on the type of
 * {@link RdfTripleStoreConfiguration} object that is passed in and whether or not
 * Rya is configured to use Mongo.
 *
 */
public class RyaQueryEngineFactory {

    
    @SuppressWarnings("unchecked")
    public static <C extends RdfTripleStoreConfiguration> RyaQueryEngine<C> getQueryEngine(RdfTripleStoreConfiguration conf) {
        if(conf instanceof AccumuloRdfConfiguration) {
            AccumuloRdfConfiguration aConf = (AccumuloRdfConfiguration) conf;
            Instance instance;
            String instanceName = aConf.get("sc.cloudbase.instancename");
            String user = aConf.get("sc.cloudbase.username");
            String password = aConf.get("sc.cloudbase.password");
            if(aConf.getBoolean(".useMockInstance", false)) {
                instance = new MockInstance(instanceName);
            } else {
                String zookeepers = aConf.get("sc.cloudbase.zookeepers");
                instance = new ZooKeeperInstance(instanceName, zookeepers);
            }
            Connector conn;
            try {
                conn = instance.getConnector(user, new PasswordToken(password));
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException(e);
            }
            return (RyaQueryEngine<C>) new AccumuloRyaQueryEngine(conn, aConf);
        } else if(conf instanceof MongoDBRdfConfiguration && conf.getBoolean("sc.useMongo", false)) {
            MongoClient client = MongoConnectorFactory.getMongoClient(conf);
            return (RyaQueryEngine<C>) new MongoDBQueryEngine((MongoDBRdfConfiguration) conf, client);
        } else {
            throw new IllegalArgumentException("Invalid configuration type.");
        }
    }
    
}
