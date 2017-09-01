/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.api.client;

/**
 * Verifies that Rya instance has Fluo application enabled and lists
 * all SPARQL queries maintained by the applcation.
 */
public interface ListIncrementalQueries {

    /**
     * Lists all SPARQL queries maintained by the Fluo Application for a given rya instance and associated information,
     * including the Fluo Query Id, the QueryType, the ExportStrategy, and the pretty-printed SPARQL query.
     * 
     * @param ryaInstance - Rya instance whose queries are incrementally maintained by Fluo
     * @return String comprised of new line delimited Strings that provide information about each query registered in
     * Fluo, including the query Id, the query type, the export strategies, and the SPARQL query
     * @throws RyaClientException
     */
    public String listIncrementalQueries(String ryaInstance) throws RyaClientException;
    
}
