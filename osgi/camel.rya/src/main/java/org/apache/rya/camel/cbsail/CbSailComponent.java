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



import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.sail.SailRepository;

import java.util.Map;

import static com.google.common.base.Preconditions.*;
/**
 * Save and retrieve triples
 */
public class CbSailComponent extends DefaultComponent {
    public static final String SAILREPONAME = "sailRepoName";
    
    public static final String ENDPOINT_URI = "cbsail";
    public static final String SPARQL_QUERY_PROP = "cbsail.sparql";
    public static final String START_TIME_QUERY_PROP = "cbsail.startTime";
    public static final String TTL_QUERY_PROP = "cbsail.ttl";
    public static final ValueFactory valueFactory = new ValueFactoryImpl();

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        String sailRepoNameParam = Repository.class.getName();
        if (parameters.containsKey(sailRepoNameParam)) {
            sailRepoNameParam = getAndRemoveParameter(parameters, SAILREPONAME, String.class);
        }
        Repository sailRepository = getCamelContext().getRegistry().lookup(sailRepoNameParam, Repository.class);
        checkNotNull(sailRepository, "Sail Repository must exist within the camel registry. Using lookup name[" + sailRepoNameParam + "]");

        CbSailEndpoint sailEndpoint = new CbSailEndpoint(uri, this, sailRepository, remaining);
        setProperties(sailEndpoint, parameters);
        return sailEndpoint;
    }
}
