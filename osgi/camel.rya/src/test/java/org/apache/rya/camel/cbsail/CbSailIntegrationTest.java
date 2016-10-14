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



import org.apache.rya.camel.cbsail.CbSailComponent;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.CamelTestSupport;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.util.HashMap;

public class CbSailIntegrationTest extends CamelTestSupport {

    @EndpointInject(uri = "cbsail:tquery?server=stratus13&port=2181&user=root&pwd=password&instanceName=stratus")
    ProducerTemplate producer;

    public void testCbSail() throws Exception {
        String underGradInfo = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
                        " PREFIX ub: <urn:test:onto:univ#>" +
                        " SELECT * WHERE" +
                        " {" +
                        "       <http://www.Department0.University0.edu/UndergraduateStudent600> ?pred ?obj ." +
                        " }";
        HashMap map = new HashMap();
        map.put(CbSailComponent.SPARQL_QUERY_PROP, underGradInfo);
        map.put(CbSailComponent.START_TIME_QUERY_PROP, 0l);
        map.put(CbSailComponent.TTL_QUERY_PROP, 86400000l);
        Object o = producer.requestBodyAndHeaders(null, map);
        System.out.println(o);
        Thread.sleep(100000);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {

            @Override
            public void configure() throws Exception {
                ValueFactory vf = new ValueFactoryImpl();
                String underGradInfo = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
                        " PREFIX ub: <urn:test:onto:univ#>" +
                        " SELECT * WHERE" +
                        " {" +
                        "       <http://www.Department0.University0.edu/UndergraduateStudent60> ?pred ?obj ." +
                        " }";
                String rawEvents = "PREFIX nh: <http://org.apache.com/2011/02/nh#>\n" +
                        " SELECT * WHERE\n" +
                        " {\n" +
                        "     ?uuid nh:timestamp ?timestamp.\n" +
                        "     ?uuid nh:site ?site;\n" +
                        "          nh:system ?system;\n" +
                        "          nh:dataSupplier ?dataSupplier;\n" +
                        "          nh:dataType ?dataType;\n" +
                        "          <http://org.apache.com/2011/02/nh#count> ?data.\n" +
                        " } LIMIT 100";
                String latestModels = "PREFIX nh: <http://org.apache.com/rdf/2011/02/model#>" +
                        " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>" +
                        " SELECT * WHERE" +
                        " {" +
                        "     ?modelUuid nh:dayOfWeek \"5\";" +
                        "          nh:hourOfDay \"3\";" +
                        "          nh:timestamp ?timestamp;" +
//                        "     FILTER (xsd:integer(?timestamp) > 1297652964633)." +
                        "          nh:dataProperty \"count\";" +
                        "          nh:modelType \"org.apache.learning.tpami.SimpleGaussianMMModel\";" +
                        "          nh:site ?site;" +
                        "          nh:dataSupplier ?dataSupplier;" +
                        "          nh:system ?system;" +
                        "          nh:dataType ?dataType;" +
                        "          nh:model ?model;" +
                        "          nh:key ?key." +
                        " }";

                from("timer://foo?fixedRate=true&period=60000").
                        setHeader(CbSailComponent.SPARQL_QUERY_PROP, constant(underGradInfo)).
//        setBody(constant(new StatementImpl(vf.createURI("http://www.Department0.University0.edu/UndergraduateStudent610"), vf.createURI("urn:test:onto:univ#testPred"), vf.createLiteral("test")))).
                        to("cbsail:tquery?server=stratus13&port=2181&user=root&pwd=password&instanceName=stratus&queryOutput=XML" +
//        "&ttl=259200000"
//        + "&sparql=" + latestModels" +
                        "").process(new Processor() {

                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println(exchange.getIn().getBody());
//                        if (body != null)
//                            System.out.println(body.size());
                    }
                }).end();
            }
        };
    }

}
