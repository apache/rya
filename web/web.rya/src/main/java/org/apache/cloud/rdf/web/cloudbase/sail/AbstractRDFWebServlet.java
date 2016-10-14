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

//package org.apache.cloud.rdf.web.cloudbase.sail;

//
//import cloudbase.core.client.Connector;
//import cloudbase.core.client.ZooKeeperInstance;
//import org.apache.rya.cloudbase.CloudbaseRdfDAO;
//import org.apache.rya.cloudbase.CloudbaseRdfEvalStatsDAO;
//import RdfCloudTripleStore;
//import org.openrdf.repository.Repository;
//import org.openrdf.repository.RepositoryException;
//import org.openrdf.repository.sail.SailRepository;
//
//import javax.servlet.ServletConfig;
//import javax.servlet.ServletException;
//import javax.servlet.http.HttpServlet;
//
///**
// * Class AbstractRDFWebServlet
// * Date: Dec 13, 2010
// * Time: 9:44:08 AM
// */
//public class AbstractRDFWebServlet extends HttpServlet implements RDFWebConstants {
//
//    protected Repository repository;
//    protected String origTablePrefix;
//
//    @Override
//    public void init(ServletConfig config) throws ServletException {
//        super.init(config);
//        try {
//            String instance = config.getInitParameter(INSTANCE_PARAM);
//            String server = config.getInitParameter(SERVER_PARAM);
//            String port = config.getInitParameter(PORT_PARAM);
//            String user = config.getInitParameter(USER_PARAM);
//            String password = config.getInitParameter(PASSWORD_PARAM);
//            String tablePrefix = config.getInitParameter(TABLEPREFIX_PARAM);
//
//            RdfCloudTripleStore rts = new RdfCloudTripleStore();
////        rts.setInstance("dne");
////        if (instance != null)
////            rts.setInstance(instance);
////        if (server != null)
////            rts.setServer(server);
////        if (port != null)
////            rts.setPort(Integer.parseInt(port));
////        if (user != null)
////            rts.setUser(user);
////        if (password != null)
////            rts.setPassword(password);
////        if (tablePrefix != null) {
////            rts.setTablePrefix(tablePrefix);
////            origTablePrefix = tablePrefix;
////        }
//            CloudbaseRdfDAO crdfdao = new CloudbaseRdfDAO();
//            Connector connector = new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password");
//            crdfdao.setConnector(connector);
//            crdfdao.setSpoTable("lubm_spo");
//            crdfdao.setPoTable("lubm_po");
//            crdfdao.setOspTable("lubm_osp");
//            crdfdao.setNamespaceTable("lubm_ns");
//            rts.setRdfDao(crdfdao);
//            CloudbaseRdfEvalStatsDAO ceval = new CloudbaseRdfEvalStatsDAO();
//            ceval.setConnector(connector);
//            ceval.setEvalTable("lubm_eval");
//            rts.setRdfEvalStatsDAO(ceval);
//
//            repository = new SailRepository(rts);
//
//            repository.initialize();
//        } catch (Exception e) {
//            throw new ServletException(e);
//        }
//    }
//
//    @Override
//    public void destroy() {
//        try {
//            repository.shutDown();
//        } catch (RepositoryException e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    public Repository getRepository() {
//        return repository;
//    }
//
//    public void setRepository(Repository repository) {
//        this.repository = repository;
//    }
//}
