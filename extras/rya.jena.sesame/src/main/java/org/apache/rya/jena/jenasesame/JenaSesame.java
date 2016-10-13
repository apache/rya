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
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.rya.jena.jenasesame.impl.GraphRepository;
import org.apache.rya.jena.jenasesame.impl.JenaSesameDatasetGraph;
import org.apache.rya.jena.jenasesame.impl.JenaSesameQueryEngineFactory;
import org.openrdf.model.Resource;
import org.openrdf.repository.RepositoryConnection;

/**
 * Jena API over Sesame repository
 */
public class JenaSesame {
    private static boolean isInitialized = false;
    private static QueryEngineFactory factory = new JenaSesameQueryEngineFactory();
    static {
        init ();
    }

    private static void init() {
        if (!isInitialized) {
            isInitialized = true;
            QueryEngineRegistry.addFactory(factory);
        }
    }

    /**
     * Create a Model that is backed by a repository.
     * The model is the triples seen with no specification of the context.
     * @param connection the {@link RepositoryConnection}.
     * @return the {@link Model}.
     */
    public static Model createModel(final RepositoryConnection connection) {
        final Graph graph = new GraphRepository(connection);
        return ModelFactory.createModelForGraph(graph);
    }

    /**
     * Create a model that is backed by a repository.
     * The model is the triples seen with specified context.
     * @param connection the {@link RepositoryConnection}.
     * @param context the {@link Resource} context.
     * @return the {@link Model}.
     */
    public static Model createModel(final RepositoryConnection connection, final Resource context) {
        final Graph graph =  new GraphRepository(connection, context);
        return ModelFactory.createModelForGraph(graph);
    }

    /**
     * Create a dataset that is backed by a repository
     * @param connection the {@link RepositoryConnection}.
     * @return the {@link Dataset}.
     */
    public static Dataset createDataset(final RepositoryConnection connection) {
        final DatasetGraph dsg = new JenaSesameDatasetGraph(connection);
        return DatasetFactory.wrap(dsg);
    }
}