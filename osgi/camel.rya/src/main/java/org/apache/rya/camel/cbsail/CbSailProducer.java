package org.apache.rya.camel.cbsail;

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



import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.openrdf.model.Statement;
import org.openrdf.query.*;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import java.io.ByteArrayOutputStream;
import java.util.*;

import static org.apache.rya.api.RdfCloudTripleStoreConfiguration.*;
import static org.apache.rya.camel.cbsail.CbSailComponent.SPARQL_QUERY_PROP;
import static org.apache.rya.camel.cbsail.CbSailComponent.valueFactory;

/**
 */
public class CbSailProducer extends DefaultProducer {

    private RepositoryConnection connection;

    private CbSailEndpoint.CbSailOutput queryOutput = CbSailEndpoint.CbSailOutput.BINARY;

    public CbSailProducer(CbSailEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    public void process(final Exchange exchange) throws Exception {
        //If a query is set in the header or uri, use it
        Collection<String> queries = new ArrayList<String>();
        Collection tmp = exchange.getIn().getHeader(SPARQL_QUERY_PROP, Collection.class);
        if (tmp != null) {
            queries = tmp;
        } else {
            String query = exchange.getIn().getHeader(SPARQL_QUERY_PROP, String.class);
            if (query != null) {
                queries.add(query);
            }
        }

        if (queries.size() > 0)
            sparqlQuery(exchange, queries);
        else
            inputTriples(exchange);
    }

    protected void inputTriples(Exchange exchange) throws RepositoryException {
        Object body = exchange.getIn().getBody();
        if (body instanceof Statement) {
            //save statement
            inputStatement((Statement) body);
        } else if (body instanceof List) {
            //save list of statements
            List lst = (List) body;
            for (Object obj : lst) {
                if (obj instanceof Statement)
                    inputStatement((Statement) obj);
            }
        }
        connection.commit();
        exchange.getOut().setBody(Boolean.TRUE);
    }

    protected void inputStatement(Statement stmt) throws RepositoryException {
        connection.add(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
    }

    protected void sparqlQuery(Exchange exchange, Collection<String> queries) throws RepositoryException, MalformedQueryException, QueryEvaluationException, TupleQueryResultHandlerException, RDFHandlerException {

        List list = new ArrayList();
        for (String query : queries) {

//            Long startTime = exchange.getIn().getHeader(START_TIME_QUERY_PROP, Long.class);
//            Long ttl = exchange.getIn().getHeader(TTL_QUERY_PROP, Long.class);
            String auth = exchange.getIn().getHeader(CONF_QUERY_AUTH, String.class);
            Boolean infer = exchange.getIn().getHeader(CONF_INFER, Boolean.class);

            Object output = performSelect(query, auth, infer);
            if (queries.size() == 1) {
                exchange.getOut().setBody(output);
                return;
            } else
                list.add(output);

        }
        exchange.getOut().setBody(list);
    }

    protected Object performSelect(String query, String auth, Boolean infer) throws RepositoryException, MalformedQueryException, QueryEvaluationException, TupleQueryResultHandlerException {
        TupleQuery tupleQuery = connection.prepareTupleQuery(
                QueryLanguage.SPARQL, query);
        if (auth != null && auth.length() > 0)
            tupleQuery.setBinding(CONF_QUERY_AUTH, valueFactory.createLiteral(auth));
        if (infer != null)
            tupleQuery.setBinding(CONF_INFER, valueFactory.createLiteral(infer));
        if (CbSailEndpoint.CbSailOutput.BINARY.equals(queryOutput)) {
            final List listOutput = new ArrayList();
            TupleQueryResultHandlerBase handler = new TupleQueryResultHandlerBase() {
                @Override
                public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
                    Map<String, String> map = new HashMap<String, String>();
                    for (String s : bindingSet.getBindingNames()) {
                        map.put(s, bindingSet.getBinding(s).getValue().stringValue());
                    }
                    listOutput.add(map);
                }
            };
            tupleQuery.evaluate(handler);
            return listOutput;
        } else if (CbSailEndpoint.CbSailOutput.XML.equals(queryOutput)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            SPARQLResultsXMLWriter sparqlWriter = new SPARQLResultsXMLWriter(baos);
            tupleQuery.evaluate(sparqlWriter);
            return new String(baos.toByteArray());
        } else {
            throw new IllegalArgumentException("Query Output[" + queryOutput + "] is not recognized");
        }
    }

//    protected Object performConstruct(String query, Long ttl, Long startTime) throws RepositoryException, MalformedQueryException, QueryEvaluationException, TupleQueryResultHandlerException, RDFHandlerException {
//        GraphQuery tupleQuery = connection.prepareGraphQuery(
//                QueryLanguage.SPARQL, query);
//        if (ttl != null && ttl > 0)
//            tupleQuery.setBinding("ttl", valueFactory.createLiteral(ttl));
//        if (startTime != null && startTime > 0)
//            tupleQuery.setBinding("startTime", valueFactory.createLiteral(startTime));
//        if (CbSailEndpoint.CbSailOutput.BINARY.equals(queryOutput)) {
//            throw new IllegalArgumentException("In Graph Construct mode, cannot return Java object");
//        } else if (CbSailEndpoint.CbSailOutput.XML.equals(queryOutput)) {
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            RDFXMLWriter rdfWriter = new RDFXMLWriter(baos);
//            tupleQuery.evaluate(rdfWriter);
//            return new String(baos.toByteArray());
//        } else {
//            throw new IllegalArgumentException("Query Output[" + queryOutput + "] is not recognized");
//        }
//    }


    @Override
    protected void doStart() throws Exception {
        CbSailEndpoint cbSailEndpoint = (CbSailEndpoint) getEndpoint();
        connection = cbSailEndpoint.getSailRepository().getConnection();
    }

    @Override
    protected void doStop() throws Exception {
        connection.close();
    }
}
