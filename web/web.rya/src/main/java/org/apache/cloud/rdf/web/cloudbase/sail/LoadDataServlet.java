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
//import org.openrdf.model.Resource;
//import org.openrdf.repository.RepositoryConnection;
//import org.openrdf.repository.RepositoryException;
//import org.openrdf.rio.RDFFormat;
//import org.openrdf.rio.RDFParseException;
//
//import javax.servlet.ServletException;
//import javax.servlet.ServletInputStream;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.io.IOException;
//
//public class LoadDataServlet extends AbstractRDFWebServlet {
//
//    @Override
//    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
//            throws ServletException, IOException {
//        if (req == null || req.getInputStream() == null)
//            return;
//
//        String format_s = req.getParameter("format");
//        RDFFormat format = RDFFormat.RDFXML;
//        if (format_s != null) {
//            format = RDFFormat.valueOf(format_s);
//            if (format == null)
//                throw new ServletException("RDFFormat[" + format_s + "] not found");
//        }
//        ServletInputStream stream = req.getInputStream();
//
//        RepositoryConnection conn = null;
//        try {
//            conn = repository.getConnection();
//
//            // generate data
//            conn.add(stream, "", format, new Resource[]{});
//            conn.commit();
//
//            conn.close();
//        } catch (RepositoryException e) {
//            throw new ServletException(e);
//        } catch (RDFParseException e) {
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
