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
import org.apache.rya.export.accumulo.common.InstanceType;

/**
 * Immutable configuration object to allow the MergeTool to connect to the parent and child
 * databases for data merging.
 */
@Immutable
public class AccumuloMergeConfiguration extends MergeConfiguration {
    /**
     * Information needed to connect to the parent database
     */
    private final String parentZookeepers;
    private final String parentAuths;
    private final InstanceType parentInstanceType;

    /**
     * Information needed to connect to the child database
     */
    private final String childZookeepers;
    private final String childAuths;
    private final InstanceType childInstanceType;

    /**
     * Constructs a {@link AccumuloMergeConfiguration}.  All fields are required.
     */
    private AccumuloMergeConfiguration(final AccumuloBuilder builder) throws MergeConfigurationException {
        super(checkNotNull(builder));
        try {
            this.parentZookeepers = checkNotNull(builder.parentZookeepers);
            this.parentAuths = checkNotNull(builder.parentAuths);
            this.parentInstanceType = checkNotNull(builder.parentInstanceType);
            this.childZookeepers = checkNotNull(builder.childZookeepers);
            this.childAuths = checkNotNull(builder.childAuths);
            this.childInstanceType = checkNotNull(builder.childInstanceType);
        } catch(final NullPointerException npe) {
            throw new MergeConfigurationException("The configuration was missing required field(s)", npe);
        }
    }

    /**
     * @return the Zookeeper host names of the parent used by Accumulo.
     */
    public String getParentZookeepers() {
        return parentZookeepers;
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
    public InstanceType getParentInstanceType() {
        return parentInstanceType;
    }

    /**
     * @return the Zookeeper host names of the child used by Accumulo.
     */
    public String getChildZookeepers() {
        return childZookeepers;
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
    public InstanceType getChildInstanceType() {
        return childInstanceType;
    }

    /**
     * Builder to help create {@link MergeConfiguration}s.
     */
    public static class AccumuloBuilder extends MergeConfiguration.Builder {
        private String parentZookeepers;
        private String parentAuths;
        private InstanceType parentInstanceType;

        private String childZookeepers;
        private String childAuths;
        private InstanceType childInstanceType;

        public AccumuloBuilder() {
            super();
        }

        /**
         * @param zookeepers - the Zookeeper host names of the parent used by
         * Accumulo.
         * @return the updated {@link AccumuloBuilder}.
         */
        public AccumuloBuilder setParentZookeepers(final String zookeepers) {
            parentZookeepers = zookeepers;
            return this;
        }

        /**
         * @param auths - the Accumulo user authorizations of the parent.
         * @return the updated {@link AccumuloBuilder}.
         */
        public AccumuloBuilder setParentAuths(final String auths) {
            parentAuths = auths;
            return this;
        }

        /**
         * @param instanceType the Accumulo instance type of the parent.
         * @return the updated {@link AccumuloBuilder}.
         */
        public AccumuloBuilder setParentInstanceType(final InstanceType instanceType) {
            parentInstanceType = instanceType;
            return this;
        }

        /**
         * @param zookeepers - the Zookeeper host names of the child used by
         * Accumulo.
         * @return the updated {@link AccumuloBuilder}.
         */
        public AccumuloBuilder setChildZookeepers(final String zookeepers) {
            childZookeepers = zookeepers;
            return this;
        }

        /**
         * @param auths - the Accumulo user authorizations of the child.
         * @return the updated {@link AccumuloBuilder}.
         */
        public AccumuloBuilder setChildAuths(final String auths) {
            childAuths = auths;
            return this;
        }

        /**
         * @params instanceType - the Accumulo instance type of the child.
         * @return the updated {@link AccumuloBuilder}.
         */
        public AccumuloBuilder setChildInstanceType(final InstanceType instanceType) {
            childInstanceType = instanceType;
            return this;
        }

        @Override
        public AccumuloMergeConfiguration build() throws MergeConfigurationException {
            return new AccumuloMergeConfiguration(this);
        }
    }
}
