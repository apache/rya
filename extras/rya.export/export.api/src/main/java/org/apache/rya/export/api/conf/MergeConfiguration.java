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
    private final String parentUsername;
    private final String parentPassword;
    private final String parentRyaInstanceName;
    private final DBType parentDBType;
    private final int parentPort;

    /**
     * Information needed to connect to the child database
     */
    private final String childHostname;
    private final String childUsername;
    private final String childPassword;
    private final String childRyaInstanceName;
    private final DBType childDBType;
    private final int childPort;

    private final MergePolicy mergePolicy;

    private final boolean useNtpServer;
    private final String ntpServerHost;

    /**
     * Constructs a MergeConfiguration.  All fields are required.
     * @param parentHostname
     * @param parentUsername
     * @param parentPassword
     * @param parentPort
     * @param parentRyaInstanceName
     * @param parentDBType
     * @param childHostname
     * @param childUsername
     * @param childPassword
     * @param childPort
     * @param childRyaInstanceName
     * @param childDBType
     * @param mergePolicy
     * @param useNtpServer
     * @param ntpServerHost
     * @throws MergeConfigurationException
     */
    private MergeConfiguration(final String parentHostname, final String parentUsername, final String parentPassword,
            final int parentPort, final String parentRyaInstanceName, final DBType parentDBType,
            final String childHostname, final String childUsername, final String childPassword,
            final int childPort, final String childRyaInstanceName, final DBType childDBType,
            final MergePolicy mergePolicy, final boolean useNtpServer, final String ntpServerHost) throws MergeConfigurationException {
        try {
            this.parentHostname = checkNotNull(parentHostname);
            this.parentUsername = checkNotNull(parentUsername);
            this.parentPassword = checkNotNull(parentPassword);
            this.parentRyaInstanceName = checkNotNull(parentRyaInstanceName);
            this.parentDBType = checkNotNull(parentDBType);
            this.parentPort = checkNotNull(parentPort);
            this.childHostname = checkNotNull(childHostname);
            this.childUsername = checkNotNull(childUsername);
            this.childPassword = checkNotNull(childPassword);
            this.childRyaInstanceName = checkNotNull(childRyaInstanceName);
            this.childDBType = checkNotNull(childDBType);
            this.childPort = checkNotNull(childPort);
            this.mergePolicy = checkNotNull(mergePolicy);
            this.useNtpServer = checkNotNull(useNtpServer);
            this.ntpServerHost = checkNotNull(ntpServerHost);
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
     * @return the username of the parent.
     */
    public String getParentUsername() {
        return parentUsername;
    }

    /**
     * @return the password of the parent.
     */
    public String getParentPassword() {
        return parentPassword;
    }

    /**
     * @return the Rya Instance Name of the parent.
     */
    public String getParentRyaInstanceName() {
        return parentRyaInstanceName;
    }

    /**
     * @return the Database Type of the parent.
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
     * @return the username of the child.
     */
    public String getChildUsername() {
        return childUsername;
    }

    /**
     * @return the password of the child.
     */
    public String getChildPassword() {
        return childPassword;
    }

    /**
     * @return the Rya Instance Name of the child.
     */
    public String getChildRyaInstanceName() {
        return childRyaInstanceName;
    }

    /**
     * @return the Database Type of the child.
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
     * @return {@code true} to use the NTP server to handle time synchronization.
     * {@code false} to not use the NTP server.
     */
    public Boolean getUseNtpServer() {
        return useNtpServer;
    }

    /**
     * @return The host name of the time server to use.
     */
    public String getNtpServerHost() {
        return ntpServerHost;
    }

    /**
     * Builder to help create {@link MergeConfiguration}s.
     */
    public static class Builder {
        private String parentHostname;
        private String parentUsername;
        private String parentPassword;
        private String parentRyaInstanceName;
        private DBType parentDBType;
        private int parentPort;

        private String childHostname;
        private String childUsername;
        private String childPassword;
        private String childRyaInstanceName;
        private DBType childDBType;
        private int childPort;

        private MergePolicy mergePolicy;

        private boolean useNtpServer;
        private String ntpServerHost;

        public Builder setParentHostname(final String hostname) {
            parentHostname = hostname;
            return this;
        }

        public Builder setParentUsername(final String username) {
            parentUsername = username;
            return this;
        }

        public Builder setParentPassword(final String password) {
            parentPassword = password;
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

        public Builder setChildUsername(final String username) {
            childUsername = username;
            return this;
        }

        public Builder setChildPassword(final String password) {
            childPassword = password;
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

        public Builder setUseNtpServer(final boolean useNtpServer) {
            this.useNtpServer = useNtpServer;
            return this;
        }

        public Builder setNtpServerHost(final String ntpServerHost) {
            this.ntpServerHost = ntpServerHost;
            return this;
        }

        /**
         * @return The {@link MergeConfiguration} based on this builder.
         * @throws MergeConfigurationException
         * @throws NullPointerException if any field as not been provided
         */
        public MergeConfiguration build() throws MergeConfigurationException {
            return new MergeConfiguration(parentHostname, parentUsername, parentPassword, parentPort, parentRyaInstanceName, parentDBType,
                childHostname, childUsername, childPassword, childPort, childRyaInstanceName, childDBType,
                mergePolicy, useNtpServer, ntpServerHost);
        }
    }
}
