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
//public class QuerySerqlDataServlet extends AbstractRDFWebServlet {
//
//    @Override
//    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
//            throws ServletException, IOException {
//        if (req == null || req.getInputStream() == null)
//            return;
//
//        String query = req.getParameter("query");
//
//        if (query == null) {
//            throw new ServletException("Please set a query");
//        }
//
//        if (query.toLowerCase().contains("select")) {
//            try {
//                performSelect(query, resp);
//            } catch (Exception e) {
//                throw new ServletException(e);
//            }
//        } else if (query.toLowerCase().contains("construct")) {
//            try {
//                performConstruct(query, resp);
//            } catch (Exception e) {
//                throw new ServletException(e);
//            }
//        } else {
//            throw new ServletException("Invalid SERQL query: " + query);
//        }
//
//    }
//
//    private void performConstruct(String query, HttpServletResponse resp)
//            throws Exception {
//        RepositoryConnection conn = null;
//        try {
//            ServletOutputStream os = resp.getOutputStream();
//            conn = repository.getConnection();
//
//            // query data
//            GraphQuery graphQuery = conn.prepareGraphQuery(
//                    QueryLanguage.SERQL, query);
//            RDFXMLWriter rdfWriter = new RDFXMLWriter(os);
//            graphQuery.evaluate(rdfWriter);
//
//            conn.close();
//        } catch (Exception e) {
//            resp.setStatus(500);
//            e.printStackTrace(new PrintStream(resp.getOutputStream()));
//            throw new ServletException(e);
//        } finally {
//            if (conn != null) {
//                try {
//                    conn.close();
//                } catch (RepositoryException e) {
//
//                }
//            }
//        }
//    }
//
//    private void performSelect(String query, HttpServletResponse resp)
//            throws Exception {
//        RepositoryConnection conn = null;
//        try {
//            ServletOutputStream os = resp.getOutputStream();
//            conn = repository.getConnection();
//
//            // query data
//            TupleQuery tupleQuery = conn.prepareTupleQuery(
//                    QueryLanguage.SERQL, query);
//            SPARQLResultsXMLWriter sparqlWriter = new SPARQLResultsXMLWriter(os);
//            tupleQuery.evaluate(sparqlWriter);
//
//            conn.close();
//        } catch (Exception e) {
//            resp.setStatus(500);
//            e.printStackTrace(new PrintStream(resp.getOutputStream()));
//            throw new ServletException(e);
//        } finally {
//            if (conn != null) {
//                try {
//                    conn.close();
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
