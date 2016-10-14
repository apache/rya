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
import org.apache.camel.ProducerTemplate;

/**
 * Class CbSailPojoMain
 * Date: May 3, 2011
 * Time: 11:20:23 PM
 */
public class CbSailPojoMain {

    @EndpointInject(uri = "cbsail:tquery?server=stratus13&port=2181&user=root&pwd=password&instanceName=stratus")
    ProducerTemplate producer;

    public void executeQuery(String sparql) {
        Object o = producer.requestBodyAndHeader(null, CbSailComponent.SPARQL_QUERY_PROP, sparql);
        System.out.println(o);
    }

    public static void main(String[] args) {
    }
}
