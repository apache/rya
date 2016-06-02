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
package org.apache.rya.indexing.pcj.storage.accumulo;

import java.util.Set;

/**
 * Create alternative variable orders for a SPARQL query based on
 * the original ordering of its results.
 */
public interface PcjVarOrderFactory {

    /**
     * Create alternative variable orders for a SPARQL query based on
     * the original ordering of its results.
     *
     * @param varOrder - The initial variable order of a SPARQL query. (not null)
     * @return A set of alternative variable orders for the original.
     */
    public Set<VariableOrder> makeVarOrders(VariableOrder varOrder);
}