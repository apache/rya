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

import java.io.BufferedReader;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration;
import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration.MongoDBIndexingConfigBuilder;
import org.apache.rya.mongodb.EmbeddedMongoFactory;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.sail.config.RyaSailFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;

import com.google.common.io.Resources;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MongoSpinIT {
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
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
    public void testSailStrategy() throws Exception {
        insertDataFile(Resources.getResource("data.ttl"), "http://example.org#");
        insertDataFile(Resources.getResource("university.ttl"), "http://example.org#");
        insertDataFile(Resources.getResource("owlrl.ttl"), "http://example.org#");
        Set<BindingSet> solutions = executeQuery(Resources.getResource("query.sparql"));
        Set<BindingSet> expected = new HashSet<>();
        Assert.assertEquals(expected, solutions);
        conf.setUseAggregationPipeline(false);
        ForwardChainSpinTool tool = new ForwardChainSpinTool();
        ToolRunner.run(conf, tool, new String[] {});
        solutions = executeQuery(Resources.getResource("query.sparql"));
        expected.add(new ListBindingSet(Arrays.asList("X", "Y"),
            VF.createURI(EX, "Alice"), VF.createURI(EX, "Department1")));
        Assert.assertEquals(expected, solutions);
        Assert.assertEquals(24, tool.getNumInferences());
    }

    @Test
    public void testPipelineStrategy() throws Exception {
        insertDataFile(Resources.getResource("data.ttl"), "http://example.org#");
        insertDataFile(Resources.getResource("university.ttl"), "http://example.org#");
        insertDataFile(Resources.getResource("owlrl.ttl"), "http://example.org#");
        Set<BindingSet> solutions = executeQuery(Resources.getResource("query.sparql"));
        Set<BindingSet> expected = new HashSet<>();
        Assert.assertEquals(expected, solutions);
        conf.setUseAggregationPipeline(true);
        ForwardChainSpinTool tool = new ForwardChainSpinTool();
        ToolRunner.run(conf, tool, new String[] {});
        solutions = executeQuery(Resources.getResource("query.sparql"));
        expected.add(new ListBindingSet(Arrays.asList("X", "Y"),
            VF.createURI(EX, "Alice"), VF.createURI(EX, "Department1")));
        Assert.assertEquals(expected, solutions);
        Assert.assertEquals(24, tool.getNumInferences());
    }

    private void insertDataFile(URL dataFile, String defaultNamespace) throws Exception {
        RDFFormat format = Rio.getParserFormatForFileName(dataFile.getFile());
        SailRepositoryConnection conn = repository.getConnection();
        try {
            conn.add(dataFile, defaultNamespace, format);
        } finally {
            closeQuietly(conn);
        }
    }

    Set<BindingSet> executeQuery(URL queryFile) throws Exception {
        SailRepositoryConnection conn = repository.getConnection();
        try {
            InputStream queryIS = queryFile.openStream();
            BufferedReader br = new BufferedReader(new java.io.InputStreamReader(queryIS, "UTF-8"));
            String query = br.lines().collect(Collectors.joining("\n"));
            br.close();
            TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            TupleQueryResult result = tupleQuery.evaluate();
            Set<BindingSet> solutions = new HashSet<>();
            while (result.hasNext()) {
                solutions.add(result.next());
            }
            return solutions;
        } finally {
            closeQuietly(conn);
        }
    }

    private static MongoDBRdfConfiguration getConf() throws Exception {
        MongoDBIndexingConfigBuilder builder = MongoIndexingConfiguration.builder().setUseMockMongo(true);
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
