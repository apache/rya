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
package org.apache.rya.api.instance;

import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;

/**
 * Used to fetch {@link RyaDetails} from a {@link RyaDetailsRepository} and
 * add them to the application's {@link Configuration}.
 */
@ParametersAreNonnullByDefault
public class RyaDetailsToConfiguration {
    private static final Logger log = Logger.getLogger(RyaDetailsToConfiguration.class);

    /**
     * Ensures the values in the {@link Configuration} do not conflict with the values in {@link RyaDetails}.
     * If they do, the values in {@link RyaDetails} take precedent and the {@link Configuration} value will
     * be overwritten.
     *
     * @param details - The {@link RyaDetails} to add to the {@link Configuration}. (not null)
     * @param conf - The {@link Configuration} to add {@link RyaDetails} to. (not null)
     */
    public static void addRyaDetailsToConfiguration(final RyaDetails details, final Configuration conf) {
        requireNonNull(details);
        requireNonNull(conf);

        checkAndSet(conf, ConfigurationFields.USE_ENTITY, details.getEntityCentricIndexDetails().isEnabled());
        checkAndSet(conf, ConfigurationFields.USE_FREETEXT, details.getFreeTextIndexDetails().isEnabled());
        checkAndSet(conf, ConfigurationFields.USE_GEO, details.getGeoIndexDetails().isEnabled());
        checkAndSet(conf, ConfigurationFields.USE_TEMPORAL, details.getTemporalIndexDetails().isEnabled());
        checkAndSet(conf, ConfigurationFields.USE_PCJ, details.getPCJIndexDetails().isEnabled());
    }

    /**
     * Ensures a Rya Client will not try to use a secondary index that is not not supported by the Rya Instance
     * it is connecting to.
     * </p>
     * If the configuration...
     * <ul>
     *   <li>provides an 'on' value for an index that is supported, then nothing changes.</li>
     *   <li>provides an 'off' value for an index that is or is not supported, then nothing changes.</li>
     *   <li>provides an 'on' value for an index that is not supported, then the index is turned
     *       off and a warning is logged.</li>
     *   <li>does not provide any value for an index, then it will be turned on if supported.</li>
     * </ul>
     *
     * @param conf - The {@link Configuration} to potentially change. (not null)
     * @param useIndexField - The field within {@code conf} that indicates if the client will utilize the index. (not null)
     * @param indexSupported - {@code true} if the Rya Instance supports the index; otherwise {@code false}.
     */
    private static void checkAndSet(final Configuration conf, final String useIndexField, final boolean indexSupported) {
        requireNonNull(conf);
        requireNonNull(useIndexField);

        final Optional<String> useIndexStr = Optional.fromNullable( conf.get(useIndexField) );

        // If no value was provided, default to using the index if it is supported.
        if(!useIndexStr.isPresent()) {
            log.info("No Rya Client configuration was provided for the " + useIndexField +
                    " index, so it is being defaulted to " + indexSupported);
            conf.setBoolean(useIndexField, indexSupported);
            return;
        }

        // If configured to use the index, but the Rya Instance does not support it, then turn it off.
        final boolean useIndex = Boolean.parseBoolean( useIndexStr.get() );
        if(useIndex && !indexSupported) {
            log.warn("The Rya Client indicates it wants to use a secondary index that the Rya Instance does not support. " +
                    "This is not allowed, so the index will be turned off. Index Configuration Field: " + useIndexField);
            conf.setBoolean(useIndexField, false);
        }

        // Otherwise use whatever the Client wants to use.
    }
}