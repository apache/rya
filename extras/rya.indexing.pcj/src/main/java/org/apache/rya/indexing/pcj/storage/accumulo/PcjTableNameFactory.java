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

import static java.util.Objects.requireNonNull;

import java.util.UUID;

/**
 * Creates Accumulo table names that may be recognized by Rya as a table that
 * holds the results of a Precomputed Join.
 */
public class PcjTableNameFactory {

    /**
     * Creates an Accumulo table names that may be recognized by Rya as a table
     * that holds the results of a Precomputed Join.
     * </p>
     * An Accumulo cluster may host more than one Rya instance. To ensure each
     * Rya instance's RDF Triples are segregated from each other, they are stored
     * within different Accumulo tables. This is accomplished by prepending a
     * {@code tablePrefix} to every table that is owned by a Rya instance. Each
     * PCJ table is owned by a specific Rya instance, so it too must be prepended
     * with the instance's {@code tablePrefix}.
     * </p>
     * When Rya scans for PCJ tables that it may use when creating execution plans,
     * it looks for any table in Accumulo that has a name starting with its
     * {@code tablePrefix} immediately followed by "INDEX". Anything following
     * that portion of the table name is just a unique identifier for the SPARQL
     * query that is being precomputed. Here's an example of what a table name
     * may look like:
     * <pre>
     *     demo_INDEX_QUERY:c8f5367c-1660-4210-a7cb-681ed004d2d9
     * </pre>
     * The "demo_INDEX" portion indicates this table is a PCJ table for the "demo_"
     * instance of Rya. The "_QUERY:c8f5367c-1660-4210-a7cb-681ed004d2d9" portion
     * could be anything at all that uniquely identifies the query that is being updated.
     *
     * @param tablePrefix - The Rya instance's table prefix. (not null)
     * @param uniqueId - The unique portion of the Rya PCJ table name. (not null)
     * @return A Rya PCJ table name built using the provided values.
     */
    public String makeTableName(final String tablePrefix, final String uniqueId) {
        return tablePrefix + "INDEX_" + uniqueId;
    }

    /**
     * Invokes {@link #makeTableName(String, String)} with a randomly generated
     * UUID as the {@code uniqueId}.
     *
     * @param tablePrefix - The Rya instance's table prefix. (not null)
     * @return A Rya PCJ table name built using the provided values.
     */
    public String makeTableName(final String tablePrefix) {
        final String uniqueId = UUID.randomUUID().toString().replaceAll("-", "");
        return makeTableName(tablePrefix, uniqueId);
    }

    /**
     * Get the PCJ ID portion of a PCJ table name.
     *
     * @param pcjTableName - The PCJ table name. (not null)
     * @return The PCJ ID that was in the table name.
     */
    public String getPcjId(final String pcjTableName) {
        requireNonNull(pcjTableName);
        return pcjTableName.split("INDEX_")[1];
    }
}