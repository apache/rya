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
//import org.openrdf.query.QueryLanguage;
//import org.openrdf.query.TupleQuery;
//import org.openrdf.query.resultio.TupleQueryResultWriter;
//import org.openrdf.repository.RepositoryConnection;
//import org.openrdf.repository.RepositoryException;
//
//import javax.servlet.ServletException;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.io.IOException;
//
//public class DeleteDataServlet extends AbstractRDFWebServlet {
//
//    @Override
//    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
//            throws ServletException, IOException {
//        if (req == null || req.getInputStream() == null)
//            return;
//
//        String query_s = req.getParameter("query");
//
//        RepositoryConnection conn = null;
//        try {
//            conn = repository.getConnection();
//            // query data
//            TupleQuery tupleQuery = conn.prepareTupleQuery(
//                    QueryLanguage.SPARQL, query_s);
//            TupleQueryResultWriter deleter = new org.apache.mmrts.rdftriplestore.QueryResultsDeleter(conn);
//            tupleQuery.evaluate(deleter);
//
//        } catch (Exception e) {
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
//}
