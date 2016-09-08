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
package org.apache.rya.export.api.conf.policy;

import static java.util.Objects.requireNonNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationDecorator;
import org.apache.rya.export.api.conf.MergeConfigurationException;

/**
 * Configuration for merging Rya Statements using timestamp.
 */
public class TimestampPolicyMergeConfiguration extends MergeConfigurationDecorator {
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSSz");
    private final Date toolStartTime;

    /**
     * Creates a new {@link TimestampPolicyMergeConfiguration}
     * @param builder - Builder used to create the configuration.
     * @throws MergeConfigurationException - The provided time is not formatted properly.
     */
    public TimestampPolicyMergeConfiguration(final TimestampPolicyBuilder builder) throws MergeConfigurationException {
        super(builder);
        requireNonNull(builder.toolStartTime);
        try {
            toolStartTime = dateFormat.parse(builder.toolStartTime);
        } catch (final ParseException e) {
            throw new MergeConfigurationException("Cannot parse the configured start time.", e);
        }
    }

    /**
     * @return The time of the data to be included in the copy/merge process.
     */
    public Date getToolStartTime() {
        return toolStartTime;
    }

    /**
     * Builder to help create {@link MergeConfiguration}s.
     */
    public static class TimestampPolicyBuilder extends Builder {
        private String toolStartTime;

        public TimestampPolicyBuilder(final Builder builder) {
            super(builder);
        }

        /**
         * @param toolStartTime - The time of the data to be included in the
         * copy/merge process.
         * @return the updated {@link Builder}.
         */
        public Builder setToolStartTime(final String toolStartTime) {
            this.toolStartTime = toolStartTime;
            return this;
        }
    }
}
