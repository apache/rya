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



import org.apache.camel.*;
import org.apache.camel.impl.DefaultEndpoint;
import org.openrdf.repository.Repository;

import static com.google.common.base.Preconditions.*;

/**
 * setHeader(SPARQL, sqarlQuery).setHeader(TTL, ttl).to("cbsail:server?port=2181&user=user&pwd=pwd&instanceName=name").getBody(<Triple Map>)
 */
public class CbSailEndpoint extends DefaultEndpoint {


    public enum CbSailOutput {
        XML, BINARY
    }

    private Long ttl;
    private Repository sailRepository;
    private String sparql;
    private String tablePrefix;
    private boolean infer = true;
    private String queryOutput = CbSailOutput.BINARY.toString();

    public CbSailEndpoint(String endpointUri, Component component, Repository sailRepository, String remaining) {
        super(endpointUri, component);
        this.sailRepository = sailRepository;
    }

    protected void validate() {
        checkNotNull(sailRepository);
    }

    @Override
    public Producer createProducer() throws Exception {
        validate();
        return new CbSailProducer(this);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        throw new RuntimeCamelException((new StringBuilder()).append("Cannot consume from a CbSailEndpoint: ").append(getEndpointUri()).toString());
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public Long getTtl() {
        return ttl;
    }

    public void setTtl(Long ttl) {
        this.ttl = ttl;
    }

    public String getSparql() {
        return sparql;
    }

    public void setSparql(String sparql) {
        this.sparql = sparql;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public boolean isInfer() {
        return infer;
    }

    public void setInfer(boolean infer) {
        this.infer = infer;
    }

    public String getQueryOutput() {
        return queryOutput;
    }

    public void setQueryOutput(String queryOutput) {
        this.queryOutput = queryOutput;
    }

    public Repository getSailRepository() {
        return sailRepository;
    }

    public void setSailRepository(Repository sailRepository) {
        this.sailRepository = sailRepository;
    }
}
