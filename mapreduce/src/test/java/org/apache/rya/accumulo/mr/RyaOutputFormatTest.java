package org.apache.rya.accumulo.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import info.aduna.iteration.CloseableIteration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.entity.EntityCentricIndex;
import org.apache.rya.indexing.accumulo.freetext.AccumuloFreeTextIndexer;
import org.apache.rya.indexing.accumulo.freetext.SimpleTokenizer;
import org.apache.rya.indexing.accumulo.freetext.Tokenizer;
import org.apache.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer;

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

public class RyaOutputFormatTest {
    private static final String CV = "test_auth";
    private static final String GRAPH = "http://example.org/graph";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String INSTANCE_NAME = RyaOutputFormatTest.class.getSimpleName() + ".rya_output";
    private static final String PREFIX = "ryaoutputformattest_";

    MockInstance instance;
    Connector connector;
    AccumuloRyaDAO dao;
    AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    Job job;
    RyaTripleContext ryaContext;

    @Before
    public void init() throws Exception {
        MRUtils.setACMock(conf, true);
        MRUtils.setACInstance(conf, INSTANCE_NAME);
        MRUtils.setACUserName(conf, USERNAME);
        MRUtils.setACPwd(conf, PASSWORD);
        MRUtils.setTablePrefix(conf, PREFIX);
        conf.setTablePrefix(PREFIX);
        conf.setAuths(CV);
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, INSTANCE_NAME);
        conf.set(ConfigUtils.CLOUDBASE_USER, USERNAME);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, PASSWORD);
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.setClass(ConfigUtils.TOKENIZER_CLASS, SimpleTokenizer.class, Tokenizer.class);
        ryaContext = RyaTripleContext.getInstance(conf);
        instance = new MockInstance(INSTANCE_NAME);
        connector = instance.getConnector(USERNAME, new PasswordToken(PASSWORD));
        job = Job.getInstance(conf);
        RyaOutputFormat.setMockInstance(job, instance.getInstanceName());
        AccumuloOutputFormat.setConnectorInfo(job, USERNAME, new PasswordToken(PASSWORD));
        AccumuloOutputFormat.setCreateTables(job, true);
        AccumuloOutputFormat.setDefaultTableName(job, PREFIX + "default");
        RyaOutputFormat.setTablePrefix(job, PREFIX);
    }

    private void write(final RyaStatement... input) throws IOException, InterruptedException {
        final RecordWriter<Writable, RyaStatementWritable> writer =
                new RyaOutputFormat.RyaRecordWriter(job.getConfiguration());
        for (final RyaStatement rstmt : input) {
            final RyaStatementWritable rsw = new RyaStatementWritable(rstmt, ryaContext);
            writer.write(new Text("unused"), rsw);
        }
        writer.close(null);
    }

    @Test
    public void testOutputFormat() throws Exception {
        final RyaStatement input = RyaStatement.builder()
            .setSubject(new RyaURI("http://www.google.com"))
            .setPredicate(new RyaURI("http://some_other_uri"))
            .setObject(new RyaURI("http://www.yahoo.com"))
            .setColumnVisibility(CV.getBytes())
            .setValue(new byte[0])
            .setContext(new RyaURI(GRAPH))
            .build();
        RyaOutputFormat.setCoreTablesEnabled(job, true);
        RyaOutputFormat.setFreeTextEnabled(job, false);
        RyaOutputFormat.setTemporalEnabled(job, false);
        RyaOutputFormat.setEntityEnabled(job, false);
        write(input);
        TestUtils.verify(connector, conf, input);
    }

    @Test
    public void testDefaultCV() throws Exception {
        final RyaStatement input = RyaStatement.builder()
            .setSubject(new RyaURI("http://www.google.com"))
            .setPredicate(new RyaURI("http://some_other_uri"))
            .setObject(new RyaURI("http://www.yahoo.com"))
            .setValue(new byte[0])
            .setContext(new RyaURI(GRAPH))
            .build();
        final RyaStatement expected = RyaStatement.builder()
            .setSubject(new RyaURI("http://www.google.com"))
            .setPredicate(new RyaURI("http://some_other_uri"))
            .setObject(new RyaURI("http://www.yahoo.com"))
            .setValue(new byte[0])
            .setContext(new RyaURI(GRAPH))
            .setColumnVisibility(CV.getBytes())
            .build();
        RyaOutputFormat.setCoreTablesEnabled(job, true);
        RyaOutputFormat.setFreeTextEnabled(job, false);
        RyaOutputFormat.setTemporalEnabled(job, false);
        RyaOutputFormat.setEntityEnabled(job, false);
        RyaOutputFormat.setDefaultVisibility(job, CV);
        write(input);
        TestUtils.verify(connector, conf, expected);
    }

    @Test
    public void testDefaultGraph() throws Exception {
        final RyaStatement input = RyaStatement.builder()
            .setSubject(new RyaURI("http://www.google.com"))
            .setPredicate(new RyaURI("http://some_other_uri"))
            .setObject(new RyaURI("http://www.yahoo.com"))
            .setValue(new byte[0])
            .setColumnVisibility(CV.getBytes())
            .build();
        final RyaStatement expected = RyaStatement.builder()
            .setSubject(new RyaURI("http://www.google.com"))
            .setPredicate(new RyaURI("http://some_other_uri"))
            .setObject(new RyaURI("http://www.yahoo.com"))
            .setValue(new byte[0])
            .setColumnVisibility(CV.getBytes())
            .setContext(new RyaURI(GRAPH))
            .build();
        RyaOutputFormat.setCoreTablesEnabled(job, true);
        RyaOutputFormat.setFreeTextEnabled(job, false);
        RyaOutputFormat.setTemporalEnabled(job, false);
        RyaOutputFormat.setEntityEnabled(job, false);
        RyaOutputFormat.setDefaultContext(job, GRAPH);
        write(input);
        TestUtils.verify(connector, conf, expected);
    }

    @Test
    public void testFreeTextIndexing() throws Exception {
        final AccumuloFreeTextIndexer ft = new AccumuloFreeTextIndexer();
        ft.setConf(conf);
        final RyaStatement input = RyaStatement.builder()
                .setSubject(new RyaURI(GRAPH + ":s"))
                .setPredicate(new RyaURI(GRAPH + ":p"))
                .setObject(new RyaType(XMLSchema.STRING, "one two three four five"))
                .build();
        RyaOutputFormat.setCoreTablesEnabled(job, false);
        RyaOutputFormat.setFreeTextEnabled(job, true);
        RyaOutputFormat.setTemporalEnabled(job, false);
        RyaOutputFormat.setEntityEnabled(job, false);
        write(input);
        final Set<Statement> empty = new HashSet<>();
        final Set<Statement> expected = new HashSet<>();
        expected.add(RyaToRdfConversions.convertStatement(input));
        Assert.assertEquals(expected, getSet(ft.queryText("one", new StatementConstraints())));
        Assert.assertEquals(empty, getSet(ft.queryText("!two", new StatementConstraints())));
        Assert.assertEquals(expected, getSet(ft.queryText("*r", new StatementConstraints())));
        Assert.assertEquals(empty, getSet(ft.queryText("r*", new StatementConstraints())));
        Assert.assertEquals(expected, getSet(ft.queryText("!r*", new StatementConstraints())));
        Assert.assertEquals(expected, getSet(ft.queryText("t* & !s*", new StatementConstraints())));
        ft.close();
    }

    @Test
    public void testTemporalIndexing() throws Exception {
        final TemporalInstant[] instants = {
                new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 01),
                new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 02),
                new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 03),
                new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 03)
        };
        final Statement[] statements = new Statement[instants.length];
        RyaOutputFormat.setCoreTablesEnabled(job, false);
        RyaOutputFormat.setFreeTextEnabled(job, false);
        RyaOutputFormat.setTemporalEnabled(job, true);
        RyaOutputFormat.setEntityEnabled(job, false);
        final ValueFactory vf = new ValueFactoryImpl();
        for (int i = 0; i < instants.length; i++) {
            final RyaType time = RdfToRyaConversions.convertLiteral(vf.createLiteral(instants[i].toString()));
            final RyaStatement input = RyaStatement.builder()
                    .setSubject(new RyaURI(GRAPH + ":s"))
                    .setPredicate(new RyaURI(GRAPH + ":p"))
                    .setObject(time)
                    .build();
            write(input);
            statements[i] = RyaToRdfConversions.convertStatement(input);
        }
        final AccumuloTemporalIndexer temporal = new AccumuloTemporalIndexer();
        temporal.setConf(conf);
        final Set<Statement> empty = new HashSet<>();
        final Set<Statement> head = new HashSet<>();
        final Set<Statement> tail = new HashSet<>();
        head.add(statements[0]);
        tail.add(statements[2]);
        tail.add(statements[3]);
        Assert.assertEquals(empty, getSet(temporal.queryInstantBeforeInstant(instants[0], new StatementConstraints())));
        Assert.assertEquals(empty, getSet(temporal.queryInstantAfterInstant(instants[3], new StatementConstraints())));
        Assert.assertEquals(head, getSet(temporal.queryInstantBeforeInstant(instants[1], new StatementConstraints())));
        Assert.assertEquals(tail, getSet(temporal.queryInstantAfterInstant(instants[1], new StatementConstraints())));
        temporal.close();
    }


    @Test
    public void testEntityIndexing() throws Exception {
        final EntityCentricIndex entity = new EntityCentricIndex();
        entity.setConf(conf);
        final RyaStatement input = RyaStatement.builder()
                .setSubject(new RyaURI(GRAPH + ":s"))
                .setPredicate(new RyaURI(GRAPH + ":p"))
                .setObject(new RyaURI(GRAPH + ":o"))
                .build();
        RyaOutputFormat.setCoreTablesEnabled(job, false);
        RyaOutputFormat.setFreeTextEnabled(job, false);
        RyaOutputFormat.setTemporalEnabled(job, false);
        RyaOutputFormat.setEntityEnabled(job, true);
        write(input);
        entity.close();
        final Set<Statement> expected = new HashSet<>();
        final Set<Statement> inserted = new HashSet<>();
        expected.add(RyaToRdfConversions.convertStatement(input));

        final String table = EntityCentricIndex.getTableName(conf);
        final Scanner scanner = connector.createScanner(table, new Authorizations(CV));
        for (final Map.Entry<Key, Value> row : scanner) {
            System.out.println(row);
            inserted.add(RyaToRdfConversions.convertStatement(
                    EntityCentricIndex.deserializeStatement(row.getKey(), row.getValue())));
        }
        Assert.assertEquals(expected, inserted);
    }

    private static <X> Set<X> getSet(final CloseableIteration<X, ?> iter) throws Exception {
        final Set<X> set = new HashSet<X>();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        return set;
    }
}
