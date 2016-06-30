package mvm.rya.accumulo.mr;

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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.api.resolver.RyaTripleContext;
import mvm.rya.indexing.StatementConstraints;
import mvm.rya.indexing.TemporalInstant;
import mvm.rya.indexing.TemporalInstantRfc3339;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.accumulo.entity.EntityCentricIndex;
import mvm.rya.indexing.accumulo.freetext.AccumuloFreeTextIndexer;
import mvm.rya.indexing.accumulo.freetext.SimpleTokenizer;
import mvm.rya.indexing.accumulo.freetext.Tokenizer;
import mvm.rya.indexing.accumulo.geo.GeoConstants;
import mvm.rya.indexing.accumulo.geo.GeoMesaGeoIndexer;
import mvm.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer;

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

    private void write(RyaStatement... input) throws IOException, InterruptedException {
        RecordWriter<Writable, RyaStatementWritable> writer =
                new RyaOutputFormat.RyaRecordWriter(job.getConfiguration());
        for (RyaStatement rstmt : input) {
            RyaStatementWritable rsw = new RyaStatementWritable(rstmt, ryaContext);
            writer.write(new Text("unused"), rsw);
        }
        writer.close(null);
    }

    @Test
    public void testOutputFormat() throws Exception {
        RyaStatement input = RyaStatement.builder()
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
        RyaOutputFormat.setGeoEnabled(job, false);
        RyaOutputFormat.setEntityEnabled(job, false);
        write(input);
        TestUtils.verify(connector, conf, input);
    }

    @Test
    public void testDefaultCV() throws Exception {
        RyaStatement input = RyaStatement.builder()
            .setSubject(new RyaURI("http://www.google.com"))
            .setPredicate(new RyaURI("http://some_other_uri"))
            .setObject(new RyaURI("http://www.yahoo.com"))
            .setValue(new byte[0])
            .setContext(new RyaURI(GRAPH))
            .build();
        RyaStatement expected = RyaStatement.builder()
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
        RyaOutputFormat.setGeoEnabled(job, false);
        RyaOutputFormat.setEntityEnabled(job, false);
        RyaOutputFormat.setDefaultVisibility(job, CV);
        write(input);
        TestUtils.verify(connector, conf, expected);
    }

    @Test
    public void testDefaultGraph() throws Exception {
        RyaStatement input = RyaStatement.builder()
            .setSubject(new RyaURI("http://www.google.com"))
            .setPredicate(new RyaURI("http://some_other_uri"))
            .setObject(new RyaURI("http://www.yahoo.com"))
            .setValue(new byte[0])
            .setColumnVisibility(CV.getBytes())
            .build();
        RyaStatement expected = RyaStatement.builder()
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
        RyaOutputFormat.setGeoEnabled(job, false);
        RyaOutputFormat.setEntityEnabled(job, false);
        RyaOutputFormat.setDefaultContext(job, GRAPH);
        write(input);
        TestUtils.verify(connector, conf, expected);
    }

    @Test
    public void testFreeTextIndexing() throws Exception {
        AccumuloFreeTextIndexer ft = new AccumuloFreeTextIndexer();
        ft.setConf(conf);
        RyaStatement input = RyaStatement.builder()
                .setSubject(new RyaURI(GRAPH + ":s"))
                .setPredicate(new RyaURI(GRAPH + ":p"))
                .setObject(new RyaType(XMLSchema.STRING, "one two three four five"))
                .build();
        RyaOutputFormat.setCoreTablesEnabled(job, false);
        RyaOutputFormat.setFreeTextEnabled(job, true);
        RyaOutputFormat.setTemporalEnabled(job, false);
        RyaOutputFormat.setGeoEnabled(job, false);
        RyaOutputFormat.setEntityEnabled(job, false);
        write(input);
        Set<Statement> empty = new HashSet<>();
        Set<Statement> expected = new HashSet<>();
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
        TemporalInstant[] instants = {
                new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 01),
                new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 02),
                new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 03),
                new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 03)
        };
        Statement[] statements = new Statement[instants.length];
        RyaOutputFormat.setCoreTablesEnabled(job, false);
        RyaOutputFormat.setFreeTextEnabled(job, false);
        RyaOutputFormat.setTemporalEnabled(job, true);
        RyaOutputFormat.setGeoEnabled(job, false);
        RyaOutputFormat.setEntityEnabled(job, false);
        ValueFactory vf = new ValueFactoryImpl();
        for (int i = 0; i < instants.length; i++) {
            RyaType time = RdfToRyaConversions.convertLiteral(vf.createLiteral(instants[i].toString()));
            RyaStatement input = RyaStatement.builder()
                    .setSubject(new RyaURI(GRAPH + ":s"))
                    .setPredicate(new RyaURI(GRAPH + ":p"))
                    .setObject(time)
                    .build();
            write(input);
            statements[i] = RyaToRdfConversions.convertStatement(input);
        }
        AccumuloTemporalIndexer temporal = new AccumuloTemporalIndexer();
        temporal.setConf(conf);
        Set<Statement> empty = new HashSet<>();
        Set<Statement> head = new HashSet<>();
        Set<Statement> tail = new HashSet<>();
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
    public void testGeoIndexing() throws Exception {
        GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
        Point p1 = gf.createPoint(new Coordinate(1, 1));
        Point p2 = gf.createPoint(new Coordinate(2, 2));
        GeoMesaGeoIndexer geo = new GeoMesaGeoIndexer();
        geo.setConf(conf);
        RyaStatement input = RyaStatement.builder()
                .setSubject(new RyaURI(GRAPH + ":s"))
                .setPredicate(new RyaURI(GRAPH + ":p"))
                .setObject(new RyaType(GeoConstants.XMLSCHEMA_OGC_WKT, "Point(2 2)"))
                .build();
        RyaOutputFormat.setCoreTablesEnabled(job, false);
        RyaOutputFormat.setFreeTextEnabled(job, false);
        RyaOutputFormat.setTemporalEnabled(job, false);
        RyaOutputFormat.setGeoEnabled(job, true);
        RyaOutputFormat.setEntityEnabled(job, false);
        write(input);
        Set<Statement> expected = new HashSet<>();
        Assert.assertEquals(expected, getSet(geo.queryContains(p1, new StatementConstraints())));
        expected.add(RyaToRdfConversions.convertStatement(input));
        Assert.assertEquals(expected, getSet(geo.queryEquals(p2, new StatementConstraints())));
        geo.close();
    }

    @Test
    public void testEntityIndexing() throws Exception {
        EntityCentricIndex entity = new EntityCentricIndex();
        entity.setConf(conf);
        RyaStatement input = RyaStatement.builder()
                .setSubject(new RyaURI(GRAPH + ":s"))
                .setPredicate(new RyaURI(GRAPH + ":p"))
                .setObject(new RyaURI(GRAPH + ":o"))
                .build();
        RyaOutputFormat.setCoreTablesEnabled(job, false);
        RyaOutputFormat.setFreeTextEnabled(job, false);
        RyaOutputFormat.setTemporalEnabled(job, false);
        RyaOutputFormat.setGeoEnabled(job, false);
        RyaOutputFormat.setEntityEnabled(job, true);
        write(input);
        entity.close();
        Set<Statement> expected = new HashSet<>();
        Set<Statement> inserted = new HashSet<>();
        expected.add(RyaToRdfConversions.convertStatement(input));
        String table = ConfigUtils.getEntityTableName(conf);
        Scanner scanner = connector.createScanner(table, new Authorizations(CV));
        for (Map.Entry<Key, Value> row : scanner) {
            System.out.println(row);
            inserted.add(RyaToRdfConversions.convertStatement(
                    EntityCentricIndex.deserializeStatement(row.getKey(), row.getValue())));
        }
        Assert.assertEquals(expected, inserted);
    }

    private static <X> Set<X> getSet(CloseableIteration<X, ?> iter) throws Exception {
        Set<X> set = new HashSet<X>();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        return set;
    }
}
