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



import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.namespace.NamespaceManager;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.CamelTestSupport;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class CbSailTest extends CamelTestSupport {

    static String litdupsNS = "urn:test:litdups#";

    private RdfCloudTripleStore store;
    private Repository repository;
    private ValueFactory vf = RdfCloudTripleStoreConstants.VALUE_FACTORY;

    @EndpointInject(uri = "mock:results")
    protected MockEndpoint resultEndpoint;

    @Produce(uri = "direct:query")
    protected ProducerTemplate template;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        repository.shutDown();
    }

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        store = new MockRdfCloudStore();
//        store.setDisplayQueryPlan(true);
//        store.setInferencing(false);
        NamespaceManager nm = new NamespaceManager(store.getRyaDAO(), store.getConf());
        store.setNamespaceManager(nm);
        repository = new RyaSailRepository(store);
        repository.initialize();

        JndiRegistry registry = super.createRegistry();
        registry.bind(Repository.class.getName(), repository);
        return registry;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {

            @Override
            public void configure() throws Exception {
                from("direct:query").
                        to("cbsail:queryEndpoint").
                        to("mock:results");
            }
        };
    }
    
    public void testSimpleQuery() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI cpu = vf.createURI(litdupsNS, "cpu");
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        URI uri1 = vf.createURI(litdupsNS, "uri1");
        conn.add(cpu, loadPerc, uri1);
        conn.commit();
        conn.close();

        resultEndpoint.expectedMessageCount(1);

        //query through camel
        String query = "select * where {" +
                "<" + cpu.toString() + "> ?p ?o1." +
                "}";
        template.sendBodyAndHeader(null, CbSailComponent.SPARQL_QUERY_PROP, query);

        assertMockEndpointsSatisfied();
    }

    public void testSimpleQueryAuth() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI cpu = vf.createURI(litdupsNS, "cpu");
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        URI uri1 = vf.createURI(litdupsNS, "uri1");
        URI uri2 = vf.createURI(litdupsNS, "uri2");
        URI auth1 = vf.createURI(RdfCloudTripleStoreConstants.AUTH_NAMESPACE, "auth1");
        conn.add(cpu, loadPerc, uri1, auth1);
        conn.add(cpu, loadPerc, uri2);
        conn.commit();
        conn.close();

        resultEndpoint.expectedMessageCount(1);

        //query through camel
        String query = "select * where {" +
                "<" + cpu.toString() + "> ?p ?o1." +
                "}";
        template.sendBodyAndHeader(null, CbSailComponent.SPARQL_QUERY_PROP, query);

        assertMockEndpointsSatisfied();

        resultEndpoint.expectedMessageCount(2);

        query = "select * where {" +
                "<" + cpu.toString() + "> ?p ?o1." +
                "}";
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put(CbSailComponent.SPARQL_QUERY_PROP, query);
        headers.put(RdfCloudTripleStoreConfiguration.BINDING_AUTH, "auth1");
        template.sendBodyAndHeaders(null, headers);

        assertMockEndpointsSatisfied();
    }
    
    public void testInsertData() throws Exception {
        URI cpu = vf.createURI(litdupsNS, "cpu");
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        URI uri1 = vf.createURI(litdupsNS, "uri1");
        URI uri2 = vf.createURI(litdupsNS, "uri2");
        List<Statement> insert = new ArrayList<Statement>();
        insert.add(new StatementImpl(cpu, loadPerc, uri1));
        insert.add(new StatementImpl(cpu, loadPerc, uri2));

        resultEndpoint.expectedBodiesReceived(true);
        template.sendBody(insert);
        assertMockEndpointsSatisfied();

        resultEndpoint.expectedMessageCount(2);
        String query = "select * where {" +
                "<" + cpu.toString() + "> ?p ?o1." +
                "}";
        template.sendBodyAndHeader(null, CbSailComponent.SPARQL_QUERY_PROP, query);
        assertMockEndpointsSatisfied();
    }

    public class MockRdfCloudStore extends RdfCloudTripleStore {

        public MockRdfCloudStore() {
            super();
            Instance instance = new MockInstance();
            try {
                Connector connector = instance.getConnector("", "");
                setConf(new AccumuloRdfConfiguration());
                AccumuloRyaDAO cdao = new AccumuloRyaDAO();
                cdao.setConnector(connector);
                setRyaDAO(cdao);
                inferenceEngine = new InferenceEngine();
                inferenceEngine.setRyaDAO(cdao);
                inferenceEngine.setRefreshGraphSchedule(1000); //every sec
                setInferenceEngine(inferenceEngine);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
