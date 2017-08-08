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
package org.apache.rya.indexing.pcj.fluo.app;

import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;

public class IncrementalUpdateConstants {

    // String constants used to create more easily parsed patterns.
    public static final String DELIM = ":::";
    public static final String VAR_DELIM = ";";
    public static final String NODEID_BS_DELIM = "<<:>>";
    public static final String JOIN_DELIM = "<:>J<:>";
    public static final String TYPE_DELIM = "<<~>>";

    //to be used in construction of id for each node
    public static final String SP_PREFIX = "STATEMENT_PATTERN";
    public static final String JOIN_PREFIX = "JOIN";
    public static final String FILTER_PREFIX = "FILTER";
    public static final String AGGREGATION_PREFIX = "AGGREGATION";
    public static final String QUERY_PREFIX = "QUERY";
    public static final String PROJECTION_PREFIX = "PROJECTION";
    public static final String CONSTRUCT_PREFIX = "CONSTRUCT";
    public static final String PERIODIC_QUERY_PREFIX = "PERIODIC_QUERY";
    
    //binding name reserved for periodic bin id for periodic query results
    public static final String PERIODIC_BIN_ID = PeriodicQueryResultStorage.PeriodicBinId;

    public static final String URI_TYPE = "http://www.w3.org/2001/XMLSchema#anyURI";
}