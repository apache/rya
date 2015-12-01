package mvm.rya.accumulo;

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



import java.util.List;

import mvm.rya.accumulo.experimental.AccumuloIndexer;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/25/12
 * Time: 3:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccumuloRdfConfiguration extends RdfCloudTripleStoreConfiguration {

    public static final String MAXRANGES_SCANNER = "ac.query.maxranges";
    
    public static final String CONF_ADDITIONAL_INDEXERS = "ac.additional.indexers";

    public AccumuloRdfConfiguration() {
        super();
    }

    public AccumuloRdfConfiguration(Configuration other) {
        super(other);
    }

    @Override
    public AccumuloRdfConfiguration clone() {
        return new AccumuloRdfConfiguration(this);
    }

    public Authorizations getAuthorizations() {
        String[] auths = getAuths();
        if (auths == null || auths.length == 0)
            return AccumuloRdfConstants.ALL_AUTHORIZATIONS;
        return new Authorizations(auths);
    }

    public void setMaxRangesForScanner(Integer max) {
        setInt(MAXRANGES_SCANNER, max);
    }

    public Integer getMaxRangesForScanner() {
        return getInt(MAXRANGES_SCANNER, 2);
    }

    public void setAdditionalIndexers(Class<? extends AccumuloIndexer>... indexers) {
        List<String> strs = Lists.newArrayList();
        for (Class ai : indexers){
            strs.add(ai.getName());
        }
        
        setStrings(CONF_ADDITIONAL_INDEXERS, strs.toArray(new String[]{}));
    }

    public List<AccumuloIndexer> getAdditionalIndexers() {
        return getInstances(CONF_ADDITIONAL_INDEXERS, AccumuloIndexer.class);
    }
}
