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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;

import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.client.ListInstances;
import org.apache.rya.api.client.RyaClientException;

/**
 * An Accumulo implementation of the {@link ListInstances} command.
 */
@ParametersAreNonnullByDefault
public class AccumuloListInstances extends AccumuloCommand implements ListInstances {

    private final Pattern spoPattern = Pattern.compile("(.*)" + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
    private final Pattern ospPattern = Pattern.compile("(.*)" + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
    private final Pattern poPattern = Pattern.compile("(.*)" + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);

    /**
     * Constructs an instance of {@link AccumuloListInstances}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloListInstances(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
    }

    @Override
    public List<String> listInstances() throws RyaClientException {
        // Figure out what the instance names might be.
        final Map<String, InstanceTablesFound> proposedInstanceNames = new HashMap<>();

        for(final String table : getConnector().tableOperations().list()) {
            // SPO table
            final Matcher spoMatcher = spoPattern.matcher(table);
            if(spoMatcher.find()) {
                final String instanceName = spoMatcher.group(1);
                makeOrGetInstanceTables(proposedInstanceNames, instanceName).setSpoFound();
            }

            // OSP table
            final Matcher ospMatcher = ospPattern.matcher(table);
            if(ospMatcher.find()) {
                final String instanceName = ospMatcher.group(1);
                makeOrGetInstanceTables(proposedInstanceNames, instanceName).setOspFound();
            }

            // PO table
            final Matcher poMatcher = poPattern.matcher(table);
            if(poMatcher.find()) {
                final String instanceName = poMatcher.group(1);
                makeOrGetInstanceTables(proposedInstanceNames, instanceName).setPoFound();
            }
        }

        // Determine which of the proposed names fit the expected Rya table structures.
        final List<String> instanceNames = new ArrayList<>();
        for(final Entry<String, InstanceTablesFound> entry : proposedInstanceNames.entrySet()) {
            final InstanceTablesFound tables = entry.getValue();
            if(tables.allFlagsSet()) {
                instanceNames.add(entry.getKey());
            }
        }

        return instanceNames;
    }

    private InstanceTablesFound makeOrGetInstanceTables(final Map<String, InstanceTablesFound> lookup, final String instanceName) {
        if(!lookup.containsKey(instanceName)) {
            lookup.put(instanceName, new InstanceTablesFound());
        }
        return lookup.get(instanceName);
    }

    /**
     * Flags that are used to determine if a String is a Rya Instance name.
     */
    @ParametersAreNonnullByDefault
    private static class InstanceTablesFound {
        private boolean spoFound = false;
        private boolean ospFound = false;
        private boolean poFound = false;

        /**
         * Sets the SPO table as seen.
         */
        public void setSpoFound() {
            spoFound = true;
        }

        /**
         * Sets the OSP table as seen.
         */
        public void setOspFound() {
            ospFound = true;
        }

        /**
         * Sets the POS table as seen.
         */
        public void setPoFound() {
            poFound = true;
        }

        /**
         * @return {@code true} if all of the flags have been set; otherwise {@code false}.
         */
        public boolean allFlagsSet() {
            return spoFound && ospFound && poFound;
        }
    }
}