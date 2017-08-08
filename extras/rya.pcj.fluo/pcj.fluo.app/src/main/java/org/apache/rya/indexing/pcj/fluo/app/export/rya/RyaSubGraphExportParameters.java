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
import java.util.Optional;

import org.apache.fluo.api.config.FluoConfiguration;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * This class manages the parameters used to construct the RyaSubGraphExporter.
 *
 */
public class RyaSubGraphExportParameters extends RyaExportParameters {

    public static final String CONF_FLUO_INSTANCE = "pcj.fluo.export.rya.fluo.instance";
    public static final String CONF_FLUO_INSTANCE_ZOOKEEPERS = "pcj.fluo.export.rya.fluo.instance.zookeepers";
    public static final String CONF_FLUO_TABLE_NAME = "pcj.fluo.export.rya.fluo.table.name";
    public static final String CONF_USE_RYA_SUBGRAPH_EXPORTER = "pcj.fluo.export.rya.subgraph.enabled";
    
    
    public RyaSubGraphExportParameters(Map<String, String> params) {
        super(params);
    }
    
    /**
     * @param useExporter - indicates whether to use the {@link RyaSubGraphExporter}
     */
    public void setUseRyaSubGraphExporter(boolean useExporter) {
        setBoolean(params, CONF_USE_RYA_SUBGRAPH_EXPORTER, useExporter);
    }
    
    /**
     * @return boolean indicating whether to use the {@link RyaSubGraphExporter}
     */
    public boolean getUseRyaSubGraphExporter() {
        return getBoolean(params, CONF_USE_RYA_SUBGRAPH_EXPORTER, false);
    }
    
    /**
     * @param fluoInstance - the Accumulo instance that Fluo is running on
     */
    public void setFluoInstanceName(String fluoInstance) {
        params.put(CONF_FLUO_INSTANCE, Preconditions.checkNotNull(fluoInstance));
    }
    
    /**
     * @return the Accumulo instance that Fluo is running on
     */
    public Optional<String> getFluoInstanceName() {
        return Optional.ofNullable(params.get(CONF_FLUO_INSTANCE));
    }
    
    /**
     * @param fluoTable - the name of the Accumulo Fluo table
     */
    public void setFluoTable(@Nullable String fluoTable) {
        params.put(CONF_FLUO_TABLE_NAME, fluoTable);
    }
    
    /**
     * @return the name of the Accumulo Fluo table
     */
    public Optional<String> getFluoTable() {
        return Optional.ofNullable(params.get(CONF_FLUO_TABLE_NAME));
    }
    
    /**
     * @param zookeepers - the zookeepers for the Fluo instance
     */
    public void setFluoZookeepers(@Nullable String zookeepers) {
        params.put(CONF_FLUO_INSTANCE_ZOOKEEPERS, zookeepers);
    }
    
    /**
     * @return - the zookeepers for the Fluo instance
     */
    public Optional<String> getFLuoZookeepers() {
        return Optional.ofNullable(params.get(CONF_FLUO_INSTANCE_ZOOKEEPERS));
    }
    
    /**
     * Uses underlying parameter map to build a FluoConfiguration object
     * @return - FluoConfiguration for creating a FluoClient
     */
    public FluoConfiguration getFluoConfiguration() {
        final FluoConfiguration config = new FluoConfiguration();
        config.setMiniStartAccumulo(false);
        config.setAccumuloInstance(params.get(CONF_ACCUMULO_INSTANCE_NAME));
        config.setAccumuloUser(params.get(CONF_EXPORTER_USERNAME));
        config.setAccumuloPassword(params.get(CONF_EXPORTER_PASSWORD));
        config.setInstanceZookeepers(params.get(CONF_FLUO_INSTANCE_ZOOKEEPERS));
        config.setAccumuloZookeepers(params.get(CONF_ZOOKEEPER_SERVERS));

        config.setApplicationName(params.get(CONF_FLUO_APP_NAME));
        config.setAccumuloTable(params.get(CONF_FLUO_TABLE_NAME));
        return config;
    }
    
}
