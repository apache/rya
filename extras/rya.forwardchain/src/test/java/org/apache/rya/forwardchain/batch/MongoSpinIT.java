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
package org.apache.rya.forwardchain.batch;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration;
import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration.MongoDBIndexingConfigBuilder;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.sail.config.RyaSailFactory;
import org.apache.rya.test.mongo.EmbeddedMongoFactory;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.AbstractTupleQueryResultHandler;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MongoSpinIT {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private static final String EX = "http://example.org/";

    private MongoDBRdfConfiguration conf;
    private SailRepository repository;

    @Before
    public void setup() throws Exception {
        Logger.getLogger("org.apache.rya.mongodb").setLevel(Level.WARN);
        Logger.getLogger("org.apache.rya.forwardchain").setLevel(Level.INFO);
        conf = getConf();
        repository = new SailRepository(RyaSailFactory.getInstance(conf));
    }

    @After
    public void tearDown() throws Exception {
        if (repository != null) {
            try {
                repository.shutDown();
            } catch (final RepositoryException e) {
                // quietly absorb this exception
            }
        }
    }

    @Test
    public void testNoStrategy() throws Exception {
        loadDataFiles();
        final Set<BindingSet> solutions = executeQuery(Resources.getResource("query.sparql"));
        final Set<BindingSet> expected = new HashSet<>();
        assertEquals(expected, solutions);
    }

    @Test
    public void testSailStrategy() throws Exception {
        loadDataFiles();
        conf.setUseAggregationPipeline(false);
        final ForwardChainSpinTool tool = new ForwardChainSpinTool();
        ToolRunner.run(conf, tool, new String[] {});
        final Set<BindingSet> solutions = executeQuery(Resources.getResource("query.sparql"));
        final Set<BindingSet> expected = ImmutableSet.of(new ListBindingSet(Arrays.asList("X", "Y"),
            VF.createIRI(EX, "Alice"), VF.createIRI(EX, "Department1")));
        assertEquals(expected, solutions);
        // TODO: Check if spin rules with empty WHERE clauses, such as
        // rl:scm-cls in the owlrl.ttl test file, should be included.
        assertEquals(48, tool.getNumInferences());
    }

    @Test
    public void testPipelineStrategy() throws Exception {
        loadDataFiles();
        conf.setUseAggregationPipeline(true);
        final ForwardChainSpinTool tool = new ForwardChainSpinTool();
        ToolRunner.run(conf, tool, new String[] {});
        final Set<BindingSet> solutions = executeQuery(Resources.getResource("query.sparql"));
        final Set<BindingSet> expected = ImmutableSet.of(new ListBindingSet(Arrays.asList("X", "Y"),
            VF.createIRI(EX, "Alice"), VF.createIRI(EX, "Department1")));
        assertEquals(expected, solutions);
        // TODO: Check if spin rules with empty WHERE clauses, such as
        // rl:scm-cls in the owlrl.ttl test file, should be included.
        assertEquals(41, tool.getNumInferences());
    }

    private void loadDataFiles() throws Exception {
        insertDataFile(Resources.getResource("data.ttl"), "http://example.org#");
        insertDataFile(Resources.getResource("university.ttl"), "http://example.org#");
        insertDataFile(Resources.getResource("owlrl.ttl"), "http://example.org#");
    }

    private void insertDataFile(final URL dataFile, final String defaultNamespace) throws Exception {
        final RDFFormat format = Rio.getParserFormatForFileName(dataFile.getFile()).get();
        final SailRepositoryConnection conn = repository.getConnection();
        try {
            conn.add(dataFile, defaultNamespace, format);
        } finally {
            closeQuietly(conn);
        }
    }

    private Set<BindingSet> executeQuery(final URL queryFile) throws Exception {
        final SailRepositoryConnection conn = repository.getConnection();
        try(
            final InputStream queryIS = queryFile.openStream();
            final BufferedReader br = new BufferedReader(new InputStreamReader(queryIS, StandardCharsets.UTF_8));
        ) {
            final String query = br.lines().collect(Collectors.joining("\n"));
            final TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            final Set<BindingSet> solutions = new HashSet<>();
            tupleQuery.evaluate(new AbstractTupleQueryResultHandler() {
                @Override
                public void handleSolution(final BindingSet bindingSet) throws TupleQueryResultHandlerException {
                    solutions.add(bindingSet);
                }
            });
            return solutions;
        } finally {
            closeQuietly(conn);
        }
    }

    private static MongoDBRdfConfiguration getConf() throws Exception {
        final MongoDBIndexingConfigBuilder builder = MongoIndexingConfiguration.builder().setUseMockMongo(true);
        final MongoClient c = EmbeddedMongoFactory.newFactory().newMongoClient();
        final ServerAddress address = c.getAddress();
        builder.setMongoHost(address.getHost());
        builder.setMongoPort(Integer.toString(address.getPort()));
        builder.setUseInference(false);
        c.close();
        return builder.build();
    }

    private static void closeQuietly(final SailRepositoryConnection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (final RepositoryException e) {
                // quietly absorb this exception
            }
        }
    }
}
