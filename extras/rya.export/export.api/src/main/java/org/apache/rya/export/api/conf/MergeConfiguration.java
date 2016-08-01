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
    private final String parentZookeepers;
    private final String parentRyaInstanceName;
    private final String parentTablePrefix;
    private final String parentAuths;
    private final String parentInstanceType;
    private final String parentTomcatUrl;
    private final DBType parentDBType;
    private final int parentPort;

    /**
     * Information needed to connect to the child database
     */
    private final String childHostname;
    private final String childUsername;
    private final String childPassword;
    private final String childZookeepers;
    private final String childRyaInstanceName;
    private final String childTablePrefix;
    private final String childAuths;
    private final String childInstanceType;
    private final String childTomcatUrl;
    private final DBType childDBType;
    private final int childPort;

    private final MergePolicy mergePolicy;

    private final boolean useNtpServer;
    private final String ntpServerHost;
    private final String toolStartTime;

    /**
     * Constructs a MergeConfiguration.  All fields are required.
     * @param parentHostname
     * @param parentUsername
     * @param parentPassword
     * @param parentZookeepers
     * @param parentPort
     * @param parentRyaInstanceName
     * @param parentTablePrefix
     * @param parentAuths
     * @param parentInstanceType
     * @param parentTomcatUrl
     * @param parentDBType
     * @param childHostname
     * @param childUsername
     * @param childPassword
     * @param childZookeepers
     * @param childPort
     * @param childRyaInstanceName
     * @param childTablePrefix
     * @param childAuths
     * @param childInstanceType
     * @param childTomcatUrl
     * @param childDBType
     * @param mergePolicy
     * @param useNtpServer
     * @param ntpServerHost
     * @param toolStartTime
     * @throws MergeConfigurationException
     */
    private MergeConfiguration(final String parentHostname, final String parentUsername, final String parentPassword, final String parentZookeepers,
            final int parentPort, final String parentRyaInstanceName, final String parentTablePrefix, final String parentAuths, final String parentInstanceType, final String parentTomcatUrl, final DBType parentDBType,
            final String childHostname, final String childUsername, final String childPassword, final String childZookeepers,
            final int childPort, final String childRyaInstanceName, final String childTablePrefix, final String childAuths, final String childInstanceType, final String childTomcatUrl, final DBType childDBType,
            final MergePolicy mergePolicy, final boolean useNtpServer, final String ntpServerHost, final String toolStartTime) throws MergeConfigurationException {
        try {
            this.parentHostname = checkNotNull(parentHostname);
            this.parentUsername = checkNotNull(parentUsername);
            this.parentPassword = checkNotNull(parentPassword);
            this.parentZookeepers = checkNotNull(parentZookeepers);
            this.parentRyaInstanceName = checkNotNull(parentRyaInstanceName);
            this.parentTablePrefix = checkNotNull(parentTablePrefix);
            this.parentAuths = checkNotNull(parentAuths);
            this.parentInstanceType = checkNotNull(parentInstanceType);
            this.parentTomcatUrl = checkNotNull(parentTomcatUrl);
            this.parentDBType = checkNotNull(parentDBType);
            this.parentPort = checkNotNull(parentPort);
            this.childHostname = checkNotNull(childHostname);
            this.childUsername = checkNotNull(childUsername);
            this.childPassword = checkNotNull(childPassword);
            this.childZookeepers = checkNotNull(childZookeepers);
            this.childRyaInstanceName = checkNotNull(childRyaInstanceName);
            this.childTablePrefix = checkNotNull(childTablePrefix);
            this.childAuths = checkNotNull(childAuths);
            this.childInstanceType = checkNotNull(childInstanceType);
            this.childTomcatUrl = checkNotNull(childTomcatUrl);
            this.childDBType = checkNotNull(childDBType);
            this.childPort = checkNotNull(childPort);
            this.mergePolicy = checkNotNull(mergePolicy);
            this.useNtpServer = checkNotNull(useNtpServer);
            this.ntpServerHost = checkNotNull(ntpServerHost);
            this.toolStartTime = checkNotNull(toolStartTime);
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
     * @return the Zookeeper host names of the parent.
     */
    public String getParentZookeepers() {
        return parentZookeepers;
    }

    /**
     * @return the Rya Instance Name of the parent.
     */
    public String getParentRyaInstanceName() {
        return parentRyaInstanceName;
    }

    /**
     * @return the Rya table prefix of the parent.
     */
    public String getParentTablePrefix() {
        return parentTablePrefix;
    }

    /**
     * @return the Accumulo user authorizations of the parent.
     */
    public String getParentAuths() {
        return parentAuths;
    }

    /**
     * @return the Accumulo instance type of the parent.
     */
    public String getParentInstanceType() {
        return parentInstanceType;
    }

    /**
     * @return The URL of the Apache Tomcat server web page running on the parent machine.
     */
    public String getParentTomcatUrl() {
        return parentTomcatUrl;
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
     * @return the Zookeeper host names of the child.
     */
    public String getChildZookeepers() {
        return childZookeepers;
    }

    /**
     * @return the Rya Instance Name of the child.
     */
    public String getChildRyaInstanceName() {
        return childRyaInstanceName;
    }

    /**
     * @return the Rya table prefix of the child.
     */
    public String getChildTablePrefix() {
        return childTablePrefix;
    }

    /**
     * @return the Accumulo user authorizations of the child.
     */
    public String getChildAuths() {
        return childAuths;
    }

    /**
     * @return the Accumulo instance type of the child.
     */
    public String getChildInstanceType() {
        return childInstanceType;
    }

    /**
     * @return The URL of the Apache Tomcat server web page running on the child machine.
     */
    public String getChildTomcatUrl() {
        return childTomcatUrl;
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
     * @return The time of the data to be included in the copy/merge process.
     */
    public String getToolStartTime() {
        return toolStartTime;
    }

    /**
     * Builder to help create {@link MergeConfiguration}s.
     */
    public static class Builder {
        private String parentHostname;
        private String parentUsername;
        private String parentPassword;
        private String parentZookeepers;
        private String parentRyaInstanceName;
        private String parentTablePrefix;
        private String parentAuths;
        private String parentInstanceType;
        private String parentTomcatUrl;
        private DBType parentDBType;
        private int parentPort;

        private String childHostname;
        private String childUsername;
        private String childPassword;
        private String childZookeepers;
        private String childRyaInstanceName;
        private String childTablePrefix;
        private String childAuths;
        private String childInstanceType;
        private String childTomcatUrl;
        private DBType childDBType;
        private int childPort;

        private MergePolicy mergePolicy;

        private boolean useNtpServer;
        private String ntpServerHost;
        private String toolStartTime;

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

        public Builder setParentZookeepers(final String zookeepers) {
            parentZookeepers = zookeepers;
            return this;
        }

        public Builder setParentRyaInstanceName(final String ryaInstanceName) {
            parentRyaInstanceName = ryaInstanceName;
            return this;
        }

        public Builder setParentTablePrefix(final String tablePrefix) {
            parentTablePrefix = tablePrefix;
            return this;
        }

        public Builder setParentAuths(final String auths) {
            parentAuths = auths;
            return this;
        }

        public Builder setParentInstanceType(final String instanceType) {
            parentInstanceType = instanceType;
            return this;
        }

        public Builder setParentTomcatUrl(final String tomcatUrl) {
            parentTomcatUrl = tomcatUrl;
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

        public Builder setChildZookeepers(final String zookeepers) {
            childZookeepers = zookeepers;
            return this;
        }

        public Builder setChildRyaInstanceName(final String ryaInstanceName) {
            childRyaInstanceName = ryaInstanceName;
            return this;
        }

        public Builder setChildTablePrefix(final String tablePrefix) {
            childTablePrefix = tablePrefix;
            return this;
        }

        public Builder setChildAuths(final String auths) {
            childAuths = auths;
            return this;
        }

        public Builder setChildInstanceType(final String instanceType) {
            childInstanceType = instanceType;
            return this;
        }

        public Builder setChildTomcatUrl(final String tomcatUrl) {
            childTomcatUrl = tomcatUrl;
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

        public Builder setToolStartTime(final String toolStartTime) {
            this.toolStartTime = toolStartTime;
            return this;
        }

        /**
         * @return The {@link MergeConfiguration} based on this builder.
         * @throws MergeConfigurationException
         * @throws NullPointerException if any field as not been provided
         */
        public MergeConfiguration build() throws MergeConfigurationException {
            return new MergeConfiguration(parentHostname, parentUsername, parentPassword, parentZookeepers,
                    parentPort, parentRyaInstanceName, parentTablePrefix, parentAuths, parentInstanceType, parentTomcatUrl, parentDBType,
                    childHostname, childUsername, childPassword, childZookeepers,
                    childPort, childRyaInstanceName, childTablePrefix, childAuths, childInstanceType, childTomcatUrl, childDBType,
                    mergePolicy, useNtpServer, ntpServerHost, toolStartTime);
        }
    }
}
