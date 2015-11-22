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


import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.mongodb.MongoDBRdfConfiguration;
import mvm.rya.mongodb.MongoDBRyaDAO;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.conf.Configuration;
import org.openrdf.sail.Sail;

public class RyaSailFactory {

    
    
    public static Sail getInstance(Configuration conf) throws AccumuloException, 
    AccumuloSecurityException, RyaDAOException {
        
        return getRyaSail(conf);
    }
    
   
    
    private static Sail getRyaSail(Configuration config) throws AccumuloException, AccumuloSecurityException, RyaDAOException {
        
        RdfCloudTripleStore store = new RdfCloudTripleStore();
        if (ConfigUtils.getUseMongo(config)) {
            MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration(config);
            conf.setTablePrefix(config.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX));
            ConfigUtils.setIndexers(conf);
            
            MongoDBRyaDAO crdfdao = new MongoDBRyaDAO(conf);
            crdfdao.init();
            
            conf.setDisplayQueryPlan(true);
            store.setRyaDAO(crdfdao);
        } else {
            Connector connector = ConfigUtils.getConnector(config);
            AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
            crdfdao.setConnector(connector);

            AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration(config);
            conf.setTablePrefix(config.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX)); // sets
                                                                                               // TablePrefixLayoutStrategy
            ConfigUtils.setIndexers(conf);
            conf.setDisplayQueryPlan(true);

            crdfdao.setConf(conf);
            crdfdao.init();
            store.setRyaDAO(crdfdao);
        }

        return store;
    }

    
    
}
