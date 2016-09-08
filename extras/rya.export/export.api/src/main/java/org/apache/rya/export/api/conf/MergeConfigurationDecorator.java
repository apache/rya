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

import org.apache.rya.export.DBType;
import org.apache.rya.export.MergePolicy;

/**
 * Decorator for {@link MergeConfiguration}.
 * <p>
 * The {@link MergeConfiguration} should be decorated when a new datastore
 * has specific configuration fields or a new statement merge policy
 * requires configuration.
 */
public class MergeConfigurationDecorator extends MergeConfiguration {

    /**
     * Creates a new {@link MergeConfigurationDecorator}
     * @param builder - The configuratoin builder.
     * @throws MergeConfigurationException
     */
    public MergeConfigurationDecorator(final MergeConfiguration.Builder builder) throws MergeConfigurationException {
        super(builder);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return super.equals(obj);
    }

    @Override
    public String getParentHostname() {
        return super.getParentHostname();
    }

    @Override
    public String getParentUsername() {
        return super.getParentUsername();
    }

    @Override
    public String getParentPassword() {
        return super.getParentPassword();
    }

    @Override
    public String getParentRyaInstanceName() {
        return super.getParentRyaInstanceName();
    }

    @Override
    public String getParentTablePrefix() {
        return super.getParentTablePrefix();
    }

    @Override
    public String getParentTomcatUrl() {
        return super.getParentTomcatUrl();
    }

    @Override
    public DBType getParentDBType() {
        return super.getParentDBType();
    }

    @Override
    public int getParentPort() {
        return super.getParentPort();
    }

    @Override
    public String getChildHostname() {
        return super.getChildHostname();
    }

    @Override
    public String getChildUsername() {
        return super.getChildUsername();
    }

    @Override
    public String getChildPassword() {
        return super.getChildPassword();
    }

    @Override
    public String getChildRyaInstanceName() {
        return super.getChildRyaInstanceName();
    }

    @Override
    public String getChildTablePrefix() {
        return super.getChildTablePrefix();
    }

    @Override
    public String getChildTomcatUrl() {
        return super.getChildTomcatUrl();
    }

    @Override
    public DBType getChildDBType() {
        return super.getChildDBType();
    }

    @Override
    public int getChildPort() {
        return super.getChildPort();
    }

    @Override
    public MergePolicy getMergePolicy() {
        return super.getMergePolicy();
    }

    @Override
    public Boolean getUseNtpServer() {
        return super.getUseNtpServer();
    }

    @Override
    public String getNtpServerHost() {
        return super.getNtpServerHost();
    }

    @Override
    public String toString() {
        return super.toString();
    }
}