package org.apache.rya.api.instance;

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

/**
 * The possible configuration fields used in a Rya application.
 *
 * Note: The fields used here are from ConfigUtils.java, but this project
 * doesn't have scope.
 * TODO: Refactor ConfigUtils so this class is not needed.
 */
class ConfigurationFields {
    static final String USE_GEO = "sc.use_geo";
    static final String USE_FREETEXT = "sc.use_freetext";
    static final String USE_TEMPORAL = "sc.use_temporal";
    static final String USE_ENTITY = "sc.use_entity";
    static final String USE_PCJ = "sc.use_pcj";
}
