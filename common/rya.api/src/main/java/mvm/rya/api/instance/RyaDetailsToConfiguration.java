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
package mvm.rya.api.instance;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Used to fetch {@link RyaDetails} from a {@link RyaDetailsRepository} and
 * add them to the application's {@link Configuration}.
 */
public class RyaDetailsToConfiguration {
    private static final Logger LOG = Logger.getLogger(RyaDetailsToConfiguration.class);
    /**
     * Ensures the values in the {@link Configuration} do not conflict with the values in {@link RyaDetails}.
     * If they do, the values in {@link RyaDetails} take precedent and the {@link Configuration} value will
     * be overwritten.
     *
     * @param details - The {@link RyaDetails} to add to the {@link Configuration}.
     * @param conf - The {@link Configuration} to add {@link RyaDetails} to.
     */
    public static void addRyaDetailsToConfiguration(final RyaDetails details, final Configuration conf) {
        Preconditions.checkNotNull(details);
        Preconditions.checkNotNull(conf);

        checkAndSet(conf, ConfigurationFields.USE_ENTITY, details.getEntityCentricIndexDetails().isEnabled());
        checkAndSet(conf, ConfigurationFields.USE_FREETEXT, details.getFreeTextIndexDetails().isEnabled());
        checkAndSet(conf, ConfigurationFields.USE_GEO, details.getGeoIndexDetails().isEnabled());
        checkAndSet(conf, ConfigurationFields.USE_TEMPORAL, details.getTemporalIndexDetails().isEnabled());
        checkAndSet(conf, ConfigurationFields.USE_PCJ, details.getPCJIndexDetails().isEnabled());
    }

    /**
     * Checks to see if the configuration has a value in the specified field.
     * If the value exists and does not match what is expected by the {@link RyaDetails},
     * an error will be logged and the value will be overwritten.
     * @param conf - The {@link Configuration} to potentially change.
     * @param field - The field to check and set.
     * @param value - The new value in the field (is not used if the value doesn't need to be changed).
     */
    private static void checkAndSet(final Configuration conf, final String field, final boolean value) {
        final Optional<String> opt = Optional.fromNullable(conf.get(field));
        if(opt.isPresent()) {
            final Boolean curVal = new Boolean(opt.get());
            if(curVal != value) {
                LOG.error("The user configured value in: " + field + " will be overwritten by what has been configured by the admin.");
                conf.setBoolean(field, value);
            }
        } else {
            conf.setBoolean(field, value);
        }
    }
}
