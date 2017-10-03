/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.api.client.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.ListIncrementalQueries;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import org.apache.rya.indexing.pcj.fluo.api.ListFluoQueries;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

@DefaultAnnotation(NonNull.class)
public class AccumuloListIncrementalQueries extends AccumuloCommand implements ListIncrementalQueries {

    private final GetInstanceDetails getInstanceDetails;
    
    public AccumuloListIncrementalQueries(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        getInstanceDetails = new AccumuloGetInstanceDetails(connectionDetails, connector);
    }
    
    @Override
    public String listIncrementalQueries(String instanceName) throws RyaClientException {

        requireNonNull(instanceName);
        
        final Optional<RyaDetails> ryaDetailsHolder = getInstanceDetails.getDetails(instanceName);
        final boolean ryaInstanceExists = ryaDetailsHolder.isPresent();
        if (!ryaInstanceExists) {
            throw new InstanceDoesNotExistException(String.format("The '%s' instance of Rya does not exist.", instanceName));
        }

        final PCJIndexDetails pcjIndexDetails = ryaDetailsHolder.get().getPCJIndexDetails();
        final boolean pcjIndexingEnabeld = pcjIndexDetails.isEnabled();
        if (!pcjIndexingEnabeld) {
            throw new RyaClientException(String.format("The '%s' instance of Rya does not have PCJ Indexing enabled.", instanceName));
        }

        // If a Fluo application is being used, task it with updating the PCJ.
        final Optional<FluoDetails> fluoDetailsHolder = pcjIndexDetails.getFluoDetails();
        if (fluoDetailsHolder.isPresent()) {
            final String fluoAppName = fluoDetailsHolder.get().getUpdateAppName();
            try {
                return getFluoQueryString(instanceName, fluoAppName);
            } catch (Exception e) {
                throw new RyaClientException("Problem while creating Fluo Query Strings.", e);
            } 
        } else {
            throw new RyaClientException(String.format("The '%s' instance of Rya does not have Fluo incremental updating enabled.", instanceName));
        }
    }

    
    private String getFluoQueryString(final String ryaInstance, final String fluoAppName) throws Exception {

        // Connect to the Fluo application that is updating this instance's PCJs.
        final AccumuloConnectionDetails cd = super.getAccumuloConnectionDetails();
        try(final FluoClient fluoClient = new FluoClientFactory().connect(
                cd.getUsername(),
                new String(cd.getUserPass()),
                cd.getInstanceName(),
                cd.getZookeepers(),
                fluoAppName);) {
            // Initialize the PCJ within the Fluo application.
           ListFluoQueries listQueries = new ListFluoQueries();
           List<String> queries = listQueries.listFluoQueries(fluoClient);
           return Joiner.on("\n").join(queries);
        }
    }
    
}
