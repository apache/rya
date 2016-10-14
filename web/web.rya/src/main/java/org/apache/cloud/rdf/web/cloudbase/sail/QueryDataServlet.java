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
//import RdfCloudTripleStoreConstants;
//import RdfCloudTripleStoreConstants;
//import org.openrdf.model.ValueFactory;
//import org.openrdf.model.impl.ValueFactoryImpl;
//import org.openrdf.query.GraphQuery;
//import org.openrdf.query.QueryLanguage;
//import org.openrdf.query.TupleQuery;
//import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
//import org.openrdf.repository.Repository;
//import org.openrdf.repository.RepositoryConnection;
//import org.openrdf.repository.RepositoryException;
//import org.openrdf.rio.rdfxml.RDFXMLWriter;
//
//import javax.servlet.ServletException;
//import javax.servlet.ServletOutputStream;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.io.IOException;
//import java.io.PrintStream;
//
//public class QueryDataServlet extends AbstractRDFWebServlet {
//
//    private ValueFactory vf = new ValueFactoryImpl();
//
//    @Override
//    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
//            throws ServletException, IOException {
//        if (req == null || req.getInputStream() == null)
//            return;
//
//        String query = req.getParameter("query");
//        String ttl = req.getParameter("ttl");
//        String startTime = req.getParameter("startTime");
//        String infer = req.getParameter("infer");
//        String performant = req.getParameter("performant");
//        String useStats = req.getParameter("useStats");
//        String timeUris = req.getParameter("timeUris");
//        String tablePrefix = req.getParameter("tablePrefix");
//
//        //validate infer, performant
//        if (infer != null) {
//            Boolean.parseBoolean(infer);
//        } else if (performant != null) {
//            Boolean.parseBoolean(performant);
//        }
//
//        if (query == null) {
//            throw new ServletException("Please set a query");
//        }
//        if (query.toLowerCase().contains("select")) {
//            try {
//                performSelect(query, ttl, startTime, infer, performant, useStats, timeUris, tablePrefix, resp);
//            } catch (Exception e) {
//                throw new ServletException(e);
//            }
//        } else if (query.toLowerCase().contains("construct")) {
//            try {
//                performConstruct(query, ttl, startTime, infer, performant, useStats, timeUris, tablePrefix, resp);
//            } catch (Exception e) {
//                throw new ServletException(e);
//            }
//        } else {
//            throw new ServletException("Invalid SPARQL query: " + query);
//        }
//
//    }
//
//    private void performConstruct(String query, String ttl, String startTime, String infer, String performant,
//                                  String useStats, String timeUris, String tablePrefix, HttpServletResponse resp)
//            throws Exception {
//        RepositoryConnection conn = null;
//        try {
//            ServletOutputStream os = resp.getOutputStream();
//            conn = repository.getConnection();
//
//            // query data
//            GraphQuery graphQuery = conn.prepareGraphQuery(
//                    QueryLanguage.SPARQL, query);
//            if (ttl != null && ttl.length() > 0)
//                graphQuery.setBinding("ttl", vf.createLiteral(Long.parseLong(ttl)));
//            if (startTime != null && startTime.length() > 0)
//                graphQuery.setBinding("startTime", vf.createLiteral(Long.parseLong(startTime)));
//            if (performant != null && performant.length() > 0)
//                graphQuery.setBinding("performant", vf.createLiteral(Boolean.parseBoolean(performant)));
//            if (infer != null && infer.length() > 0)
//                graphQuery.setBinding("infer", vf.createLiteral(Boolean.parseBoolean(infer)));
//            if (useStats != null && useStats.length() > 0)
//                graphQuery.setBinding("useStats", vf.createLiteral(Boolean.parseBoolean(useStats)));
//            if (timeUris != null && timeUris.length() > 0)
//                graphQuery.setBinding("timeUris", vf.createURI(timeUris));
//            if (tablePrefix != null && tablePrefix.length() > 0)
//                RdfCloudTripleStoreConstants.prefixTables(tablePrefix);
//            RDFXMLWriter rdfWriter = new RDFXMLWriter(os);
//            graphQuery.evaluate(rdfWriter);
//
//        } catch (Exception e) {
//            resp.setStatus(500);
//            e.printStackTrace(new PrintStream(resp.getOutputStream()));
//            throw new ServletException(e);
//        } finally {
//            if (conn != null) {
//                try {
//                    conn.close();
//                    RdfCloudTripleStoreConstants.prefixTables(origTablePrefix);
//                } catch (RepositoryException e) {
//
//                }
//            }
//        }
//    }
//
//    private void performSelect(String query, String ttl, String startTime, String infer, String performant,
//                               String useStats, String timeUris, String tablePrefix, HttpServletResponse resp)
//            throws Exception {
//        RepositoryConnection conn = null;
//        try {
//            ServletOutputStream os = resp.getOutputStream();
//            conn = repository.getConnection();
//
//            // query data
//            TupleQuery tupleQuery = conn.prepareTupleQuery(
//                    QueryLanguage.SPARQL, query);
//            if (ttl != null && ttl.length() > 0)
//                tupleQuery.setBinding("ttl", vf.createLiteral(Long.parseLong(ttl)));
//            if (startTime != null && startTime.length() > 0)
//                tupleQuery.setBinding("startTime", vf.createLiteral(Long.parseLong(startTime)));
//            if (performant != null && performant.length() > 0)
//                tupleQuery.setBinding("performant", vf.createLiteral(Boolean.parseBoolean(performant)));
//            if (infer != null && infer.length() > 0)
//                tupleQuery.setBinding("infer", vf.createLiteral(Boolean.parseBoolean(infer)));
//            if (useStats != null && useStats.length() > 0)
//                tupleQuery.setBinding("useStats", vf.createLiteral(Boolean.parseBoolean(useStats)));
//            if (timeUris != null && timeUris.length() > 0)
//                tupleQuery.setBinding("timeUris", vf.createURI(timeUris));
//            if (tablePrefix != null && tablePrefix.length() > 0)
//                RdfCloudTripleStoreConstants.prefixTables(tablePrefix);
//            SPARQLResultsXMLWriter sparqlWriter = new SPARQLResultsXMLWriter(os);
//            tupleQuery.evaluate(sparqlWriter);
//
//        } catch (Exception e) {
//            resp.setStatus(500);
//            e.printStackTrace(new PrintStream(resp.getOutputStream()));
//            throw new ServletException(e);
//        } finally {
//            if (conn != null) {
//                try {
//                    conn.close();
//                    RdfCloudTripleStoreConstants.prefixTables(origTablePrefix);
//                } catch (RepositoryException e) {
//
//                }
//            }
//        }
//    }
//
//    public Repository getRepository() {
//        return repository;
//    }
//
//    public void setRepository(Repository repository) {
//        this.repository = repository;
//    }
//}
