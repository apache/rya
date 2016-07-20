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
package org.apache.rya.export.api.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.http.annotation.Immutable;
import org.apache.rya.export.DBType;
import org.apache.rya.export.MergePolicy;

/**
 * Immutable configuration object to allow the MergeTool to connect to the parent and child
 * databases for data merging.
 */
@Immutable
public class MergeConfiguration {
    /**
     * Information needed to connect to the parent database
     */
    private final String parentHostname;
    private final String parentRyaInstanceName;
    private final DBType parentDBType;
    private final int parentPort;

    /**
     * information needed to connect to the child database
     */
    private final String childHostname;
    private final String childRyaInstanceName;
    private final DBType childDBType;
    private final int childPort;

    private final MergePolicy mergePolicy;

    /**
     * Constructs a MergeConfiguration.  All fields are required.
     * @param parentHostname
     * @param parentPort
     * @param parentRyaInstanceName
     * @param childHostname
     * @param childPort
     * @param childRyaInstanceName
     * @param mergePolicy
     * @throws MergeConfigurationException
     */
    private MergeConfiguration(final String parentHostname, final int parentPort, final String parentRyaInstanceName, final DBType parentDBType,
            final String childHostname, final int childPort, final String childRyaInstanceName, final DBType childDBType,
            final MergePolicy mergePolicy) throws MergeConfigurationException {
        try {
            this.parentHostname = checkNotNull(parentHostname);
            this.parentRyaInstanceName = checkNotNull(parentRyaInstanceName);
            this.parentDBType = checkNotNull(parentDBType);
            this.parentPort = checkNotNull(parentPort);
            this.childHostname = checkNotNull(childHostname);
            this.childRyaInstanceName = checkNotNull(childRyaInstanceName);
            this.childDBType = checkNotNull(childDBType);
            this.childPort = checkNotNull(childPort);
            this.mergePolicy = checkNotNull(mergePolicy);
        } catch(final NullPointerException npe) {
            throw new MergeConfigurationException("The configuration was missing required field(s)", npe);
        }
    }

    /**
     * @return the hostname of the parent.
     */
    public String getParentHostname() {
        return parentHostname;
    }

    /**
     * @return the Rya Instance Name of the parent.
     */
    public String getParentRyaInstanceName() {
        return parentRyaInstanceName;
    }

    /**
     * @return the Databse Type of the parent.
     */
    public DBType getParentDBType() {
        return parentDBType;
    }

    /**
     * @return the port of the parent.
     */
    public int getParentPort() {
        return parentPort;
    }

    /**
     * @return the hostname of the child.
     */
    public String getChildHostname() {
        return childHostname;
    }

    /**
     * @return the Rya Instance Name of the child.
     */
    public String getChildRyaInstanceName() {
        return childRyaInstanceName;
    }

    /**
     * @return the Databse Type of the child.
     */
    public DBType getChildDBType() {
        return childDBType;
    }

    /**
     * @return the port of the child.
     */
    public int getChildPort() {
        return childPort;
    }

    /**
     * @return the policy to use when merging data.
     */
    public MergePolicy getMergePolicy() {
        return mergePolicy;
    }

    /**
     * Builder to help create {@link MergeConfiguration}s.
     */
    public static class Builder {
        private String parentHostname;
        private String parentRyaInstanceName;
        private DBType parentDBType;
        private int parentPort;

        private String childHostname;
        private String childRyaInstanceName;
        private DBType childDBType;
        private int childPort;

        private MergePolicy mergePolicy;

        public Builder setParentHostname(final String hostname) {
            parentHostname = hostname;
            return this;
        }

        public Builder setParentRyaInstanceName(final String ryaInstanceName) {
            parentRyaInstanceName = ryaInstanceName;
            return this;
        }

        public Builder setParentDBType(final DBType dbType) {
            parentDBType = dbType;
            return this;
        }

        public Builder setParentPort(final int port) {
            parentPort = port;
            return this;
        }

        public Builder setChildHostname(final String hostname) {
            childHostname = hostname;
            return this;
        }

        public Builder setChildRyaInstanceName(final String ryaInstanceName) {
            childRyaInstanceName = ryaInstanceName;
            return this;
        }

        public Builder setChildDBType(final DBType dbType) {
            childDBType = dbType;
            return this;
        }

        public Builder setChildPort(final int port) {
            childPort = port;
            return this;
        }

        public Builder setMergePolicy(final MergePolicy mergePolicy) {
            this.mergePolicy = mergePolicy;
            return this;
        }

        /**
         * @return The {@link MergeConfiguration} based on this builder.
         * @throws MergeConfigurationException
         * @throws NullPointerException if any field as not been provided
         */
        public MergeConfiguration build() throws MergeConfigurationException {
            return new MergeConfiguration(parentHostname, parentPort, parentRyaInstanceName, parentDBType,
                    childHostname, childPort, childRyaInstanceName, childDBType,
                    mergePolicy);
        }
    }
}
