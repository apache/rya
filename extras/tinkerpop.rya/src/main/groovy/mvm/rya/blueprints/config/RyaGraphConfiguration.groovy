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

package mvm.rya.blueprints.config

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.rexster.config.GraphConfiguration
import com.tinkerpop.rexster.config.GraphConfigurationContext;

import mvm.rya.accumulo.AccumuloRdfConfiguration
import mvm.rya.accumulo.AccumuloRyaDAO
import mvm.rya.blueprints.sail.RyaSailGraph
import mvm.rya.rdftriplestore.RdfCloudTripleStore
import mvm.rya.rdftriplestore.inference.InferenceEngine
import org.apache.commons.configuration.Configuration
import static mvm.rya.accumulo.mr.MRUtils.*
import org.apache.commons.configuration.MapConfiguration
import mvm.rya.blueprints.sail.RyaSailEdge
import mvm.rya.blueprints.sail.RyaSailVertex
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.ZooKeeperInstance

/**
 * Date: 5/8/12
 * Time: 5:38 PM
 */
class RyaGraphConfiguration implements GraphConfiguration {

    def instance, zk, user, pwd, tablePrefix, auths, cv, ttl, mock

    public static final RyaSailGraph createGraph(Map<String, String> props) {
        if (props == null) props = [:]
        def graphConfiguration = new RyaGraphConfiguration()
        RyaGraphConfiguration.load()
        return graphConfiguration.configureGraphInstance(new GraphConfigurationContext(new MapConfiguration(props), new HashMap()))
    }

    public static void load() {
        RyaSailEdge.metaClass.getSubj = { (delegate as RyaSailEdge).getVertex(Direction.OUT).id }
        RyaSailEdge.metaClass.getPred = { (delegate as RyaSailEdge).label }
        RyaSailEdge.metaClass.getObj = { (delegate as RyaSailEdge).getVertex(Direction.IN).id }
        RyaSailEdge.metaClass.getCntxt = { (delegate as RyaSailEdge).namedGraph }
        RyaSailEdge.metaClass.getStmt = { (delegate as RyaSailEdge).rawEdge }
    }

    @Override
    public RyaSailGraph configureGraphInstance(GraphConfigurationContext context) {
        Configuration graphConfiguration = context.getProperties()
        instance = graphConfiguration.getString(AC_INSTANCE_PROP)
        zk = graphConfiguration.getString(AC_ZK_PROP)
        user = graphConfiguration.getString(AC_USERNAME_PROP)
        pwd = graphConfiguration.getString(AC_PWD_PROP)
        mock = (graphConfiguration.containsKey(AC_MOCK_PROP)) ? (graphConfiguration.getBoolean(AC_MOCK_PROP)) : (null)
        assert instance != null && (zk != null || mock != null) && user != null && pwd != null

        def ryaConfiguration = new AccumuloRdfConfiguration();
        //set other properties
        graphConfiguration.keys.each { key ->
            def val = graphConfiguration.getString(key)
            if (val != null) {
                ryaConfiguration.set(key, val)
            }
        }
        //set table prefix
        ryaConfiguration.setTablePrefix(ryaConfiguration.getTablePrefix())

        def store = new RdfCloudTripleStore();
        store.setConf(ryaConfiguration);
        def cryadao = new AccumuloRyaDAO();
        def connector = null
        if (mock) {
            connector = new MockInstance(instance).getConnector(user, pwd);
        } else {
            connector = new ZooKeeperInstance(instance, zk).getConnector(user, pwd);
        }
        cryadao.setConnector(connector);
        store.setRyaDAO(cryadao);
//        def ceval = new ();
//        ceval.setConnector(connector);
//        store.setRdfEvalStatsDAO(ceval);
        def inferenceEngine = new InferenceEngine();
        inferenceEngine.setRyaDAO(cryadao);
        store.setInferenceEngine(inferenceEngine);

        return new RyaSailGraph(store)
    }
}
