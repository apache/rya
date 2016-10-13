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
package org.apache.rya.jena.jenasesame;

import org.apache.jena.graph.Graph;
import org.apache.jena.shared.JenaException;
import org.apache.rya.jena.jenasesame.impl.GraphRepository;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

public class TestJS_Graph2 extends AbstractTestGraph2 {
    @Override
    protected Graph emptyGraph() {
        return newGraph();
    }

    private static Graph newGraph() {
        try {
            final Repository repo = new SailRepository(new MemoryStore());
            repo.initialize();
            final RepositoryConnection connection = repo.getConnection();
            return new GraphRepository(connection);
        } catch (final Exception e) {
            throw new JenaException(e);
        }
    }

    @Override
    protected void returnGraph(final Graph g) {
    }
}