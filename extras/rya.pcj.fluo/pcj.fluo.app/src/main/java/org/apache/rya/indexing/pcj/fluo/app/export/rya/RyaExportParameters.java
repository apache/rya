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
package org.apache.rya.indexing.pcj.fluo.app.export.rya;

import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.pcj.fluo.app.export.ParametersBase;

import com.google.common.base.Optional;

import org.apache.fluo.api.observer.Observer;

/**
 * Provides read/write functions to the parameters map that is passed into an
 * {@link Observer#init(io.fluo.api.observer.Observer.Context)} method related
 * to Rya PCJ exporting.
 */
@ParametersAreNonnullByDefault
public class RyaExportParameters extends ParametersBase {

    public static final String CONF_EXPORT_TO_RYA = "pcj.fluo.export.rya.enabled";
    public static final String CONF_ACCUMULO_INSTANCE_NAME = "pcj.fluo.export.rya.accumuloInstanceName";
    public static final String CONF_ZOOKEEPER_SERVERS = "pcj.fluo.export.rya.zookeeperServers";
    public static final String CONF_EXPORTER_USERNAME = "pcj.fluo.export.rya.exporterUsername";
    public static final String CONF_EXPORTER_PASSWORD = "pcj.fluo.export.rya.exporterPassword";

    public static final String CONF_RYA_INSTANCE_NAME = "pcj.fluo.export.rya.ryaInstanceName";

    /**
     * Constructs an instance of {@link RyaExportParameters}.
     *
     * @param params - The parameters object that will be read/writen to. (not null)
     */
    public RyaExportParameters(final Map<String, String> params) {
        super(params);
    }

    /**
     * @param isExportToRya - {@code True} if the Fluo application should export
     *   to Rya; otherwise {@code false}.
     */
    public void setExportToRya(final boolean isExportToRya) {
        setBoolean(params, CONF_EXPORT_TO_RYA, isExportToRya);
    }

    /**
     * @return {@code True} if the Fluo application should export to Rya; otherwise
     *   {@code false}. Defaults to {@code false} if no value is present.
     */
    public boolean isExportToRya() {
        return getBoolean(params, CONF_EXPORT_TO_RYA, false);
    }

    /**
     * @param accumuloInstanceName - The name of the Accumulo instance the exporter will connect to.
     */
    public void setAccumuloInstanceName(@Nullable final String accumuloInstanceName) {
        params.put(CONF_ACCUMULO_INSTANCE_NAME, accumuloInstanceName);
    }

    /**
     * @return The name of the Accumulo instance the exporter will connect to.
     */
    public Optional<String> getAccumuloInstanceName() {
        return Optional.fromNullable( params.get(CONF_ACCUMULO_INSTANCE_NAME) );
    }

    /**
     * @param zookeeperServers - A semicolon delimited list of Zookeeper
     *   server hostnames for the zookeepers that provide connections ot the
     *   target Accumulo instance.
     */
    public void setZookeeperServers(@Nullable final String zookeeperServers) {
        params.put(CONF_ZOOKEEPER_SERVERS, zookeeperServers);
    }

    /**
     * @return A semicolon delimited list of Zookeeper  server hostnames for the
     *   zookeepers that provide connections ot the target Accumulo instance.
     */
    public Optional<String> getZookeeperServers() {
        return Optional.fromNullable( params.get(CONF_ZOOKEEPER_SERVERS) );
    }

    /**
     * @param exporterUsername - The username that will be used to export PCJ
     *   results to the destination Accumulo table.
     */
    public void setExporterUsername(@Nullable final String exporterUsername) {
        params.put(CONF_EXPORTER_USERNAME, exporterUsername);
    }

    /**
     * @return The username that will be used to export PCJ results to the
     *   destination Accumulo table.
     */
    public Optional<String> getExporterUsername() {
        return Optional.fromNullable( params.get(CONF_EXPORTER_USERNAME) );
    }

    /**
     * @param exporterPassword - The password that will be used to export PCJ
     *   results to the destination Accummulo table.
     */
    public void setExporterPassword(@Nullable final String exporterPassword) {
        params.put(CONF_EXPORTER_PASSWORD, exporterPassword);
    }

    /**
     * @return The name of the Rya instance this application is updating.
     */
    public Optional<String> getRyaInstanceName() {
        return Optional.fromNullable( params.get(CONF_RYA_INSTANCE_NAME) );
    }

    /**
     * @param ryaInstanceName - The name of the Rya instance this application is updating.
     */
    public void setRyaInstanceName(@Nullable final String ryaInstanceName) {
        params.put(CONF_RYA_INSTANCE_NAME, ryaInstanceName);
    }

    /**
     * @return The password that will be used to export PCJ
     *   results to the destination Accummulo table.
     */
    public Optional<String> getExporterPassword() {
        return Optional.fromNullable( params.get(CONF_EXPORTER_PASSWORD) );
    }
}