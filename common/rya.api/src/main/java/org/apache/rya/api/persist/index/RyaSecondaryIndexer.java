package org.apache.rya.api.persist.index;

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

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.eclipse.rdf4j.model.IRI;

public interface RyaSecondaryIndexer extends Closeable, Flushable, Configurable {
	/**
	 * initialize after setting configuration.
	 */
    void init();
    /**
     * Returns the table name if the implementation supports it.
     * Note that some indexers use multiple tables, this only returns one.
     * TODO recommend that we deprecate this method because it's a leaky interface. 
     * @return table name as a string.
     */
    String getTableName();

    void storeStatements(Collection<RyaStatement> statements) throws IOException;

    void storeStatement(RyaStatement statement) throws IOException;

    void deleteStatement(RyaStatement stmt) throws IOException;

    void dropGraph(RyaURI... graphs);

    /**
     * @return the set of predicates indexed by the indexer.
     */
    Set<IRI> getIndexablePredicates();

    @Override
    void flush() throws IOException;

    @Override
    void close() throws IOException;
}
