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

//package mvm.rya.blueprints
//
//import com.tinkerpop.blueprints.pgm.impls.sail.SailGraph
//import com.tinkerpop.blueprints.pgm.impls.sail.SailVertex
//import com.tinkerpop.gremlin.groovy.Gremlin
//import mvm.rya.accumulo.AccumuloRdfConfiguration
//import mvm.rya.accumulo.AccumuloRdfDAO
//import mvm.rya.accumulo.AccumuloRdfEvalStatsDAO
//
//import mvm.rya.blueprints.config.RyaGraphConfiguration
//import mvm.rya.rdftriplestore.RdfCloudTripleStore
//import mvm.rya.rdftriplestore.inference.InferenceEngine
//import org.apache.accumulo.core.client.ZooKeeperInstance
//import static mvm.rya.accumulo.mr.MRUtils.*
//import static mvm.rya.api.RdfCloudTripleStoreConfiguration.CONF_QUERYPLAN_FLAG
//import static mvm.rya.api.RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX
//
///**
// * Date: 5/7/12
// * Time: 5:39 PM
// */
//class TstGremlinRya {
//    public static void main(String[] args) {
//
//        def conf = new AccumuloRdfConfiguration();

//        conf.setDisplayQueryPlan(true);
//        def store = new RdfCloudTripleStore();
//        store.setConf(conf);
//        def crdfdao = new AccumuloRdfDAO();
//        def connector = new ZooKeeperInstance("acu13", "stratus25:2181").getConnector("root", "secret");
//        crdfdao.setConnector(connector);
//        conf.setTablePrefix("l_");
//        crdfdao.setConf(conf);
//        store.setRdfDao(crdfdao);
//        def ceval = new AccumuloRdfEvalStatsDAO();
//        ceval.setConnector(connector);
//        ceval.setConf(conf);
//        store.setRdfEvalStatsDAO(ceval);
//        def inferenceEngine = new InferenceEngine();
//        inferenceEngine.setRdfDao(crdfdao);
//        inferenceEngine.setConf(conf);
//        store.setInferenceEngine(inferenceEngine);
//        store.setConf(conf);
//
//        Gremlin.load()
//        def g = new SailGraph(store)
////        def g = RyaGraphConfiguration.createGraph([(AC_INSTANCE_PROP): "acu13", (AC_ZK_PROP): "stratus25:2181",(AC_USERNAME_PROP): "root", (AC_PWD_PROP): "secret", (CONF_TBL_PREFIX): "l_", (CONF_QUERYPLAN_FLAG): "true"]);
//
//        def v = g.getVertex('http://www.Department0.University0.edu/GraduateCourse0');
//        def v2 = g.getVertex('http://www.Department0.University0.edu/GraduateCourse1');
////        v.getInEdges().each {
////            println it
////        }
////        v.getInEdges('urn:lubm:rdfts#takesCourse').each {
////            println it
////        }
////        def gc0 = g.getVertex('http://www.Department0.University0.edu/GraduateCourse0')
////        gc0.getOutEdges().each {
////            println it.getInVertex()
////        }
////
////        def gc0_lit = g.getVertex('\"GraduateCourse0\"')
////        println gc0_lit
//
//        v = g.getVertex('http://dbpedia.org/resource/Albert_Camus')
//        println v.outE.each {
//            println it.label
//        }
//
//        g.shutdown()
//
//        g = RyaGraphConfiguration.createGraph([(AC_INSTANCE_PROP): "acu13", (AC_ZK_PROP): "stratus25:2181",(AC_USERNAME_PROP): "root", (AC_PWD_PROP): "secret", (CONF_TBL_PREFIX): "l_", (CONF_QUERYPLAN_FLAG): "true"]);
//
//        def rv = g.getVertex('http://dbpedia.org/resource/Albert_Camus')
//        println rv.outE.each {
//            println it.label
//        }
//
//        v = new SailVertex(rv.getRawVertex(), rv.sailGraph)
//        println v.outE
//
//        g.shutdown()
////
////        def name = {
////            println it.name
////        }
////        println SailVertex.metaClass.properties.each(name)
////        println RyaSailVertex.metaClass.properties.each(name)
////        println RyaSailVertex.metaClass.properties.each(name)
//    }
//}
