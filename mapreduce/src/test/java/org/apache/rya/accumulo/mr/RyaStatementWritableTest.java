package org.apache.rya.accumulo.mr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openrdf.model.vocabulary.XMLSchema;
import org.junit.Assert;
import org.junit.Rule;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RyaTripleContext;

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

public class RyaStatementWritableTest {
    private static final RyaURI s1 = new RyaURI(":s");
    private static final RyaURI p1 = new RyaURI(":p");
    private static final RyaType o1 = new RyaType(XMLSchema.INTEGER, "123");
    private static final RyaURI s2 = new RyaURI(":s2");
    private static final RyaURI p2 = new RyaURI(":p2");
    private static final RyaType o2 = new RyaType(XMLSchema.STRING, "123");
    private static final RyaURI graph1 = new RyaURI("http://example.org/graph1");
    private static final RyaURI graph2 = new RyaURI("http://example.org/graph2");
    private static final byte[] cv1 = "test_visibility".getBytes();
    private static final long t1 = 1000;
    private static final long t2 = 1001;
    private static final RyaStatement rs1 = RyaStatement.builder().setSubject(s1).setPredicate(p1).setObject(o1)
            .setContext(graph1).setColumnVisibility(cv1).setQualifier("q1").setTimestamp(t1).build();
    // Equivalent:
    private static final RyaStatement rs1b = RyaStatement.builder().setSubject(s1).setPredicate(p1).setObject(o1)
            .setContext(graph1).setColumnVisibility(cv1).setQualifier("q1").setTimestamp(t1).build();
    // Differ in one way each:
    private static final RyaStatement rsGraph = RyaStatement.builder().setSubject(s1).setPredicate(p1).setObject(o1)
            .setContext(graph2).setColumnVisibility(cv1).setQualifier("q1").setTimestamp(t1).build();
    private static final RyaStatement rsCv = RyaStatement.builder().setSubject(s1).setPredicate(p1).setObject(o1)
            .setContext(graph1).setColumnVisibility(null).setQualifier("q1").setTimestamp(t1).build();
    private static final RyaStatement rsQualifier = RyaStatement.builder().setSubject(s1).setPredicate(p1).setObject(o1)
            .setContext(graph1).setColumnVisibility(cv1).setQualifier("q2").setTimestamp(t1).build();
    private static final RyaStatement rsTimestamp = RyaStatement.builder().setSubject(s1).setPredicate(p1).setObject(o1)
            .setContext(graph1).setColumnVisibility(cv1).setQualifier("q1").setTimestamp(t2).build();
    // Different triple:
    private static final RyaStatement rs2 = RyaStatement.builder().setSubject(s2).setPredicate(p2).setObject(o2)
            .setContext(graph1).setColumnVisibility(null).setQualifier("q1").setTimestamp(t1).build();

    private static final RyaTripleContext ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration());

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testEquals() throws Exception {
        RyaStatementWritable rsw1 = new RyaStatementWritable(rs1, ryaContext);
        RyaStatementWritable rsw1b = new RyaStatementWritable(rs1b, null);
        RyaStatementWritable rsw2 = new RyaStatementWritable(rs2, ryaContext);
        RyaStatementWritable rswNull = new RyaStatementWritable(null, ryaContext);
        Assert.assertEquals("Equivalent statements should be equal", rsw1, rsw1b);
        Assert.assertFalse("equals(null) should be false", rsw1.equals(null));
        Assert.assertNotEquals("Statements representing different triples are not equal", rsw1, rsw2);
        Assert.assertNotEquals("Statements representing different triples are not equal", rsw1, rswNull);
        Assert.assertNotEquals("Statements with different named graphs are not equal", rsw1,
                new RyaStatementWritable(rsGraph, ryaContext));
        Assert.assertNotEquals("Statements with different column visibilities are not equal", rsw1,
                new RyaStatementWritable(rsCv, ryaContext));
        Assert.assertNotEquals("Statements with different column qualifiers are not equal", rsw1,
                new RyaStatementWritable(rsQualifier, ryaContext));
        Assert.assertNotEquals("Statements with different timestamps are not equal", rsw1,
                new RyaStatementWritable(rsTimestamp, ryaContext));
    }

    @Test
    public void testCompareTo() throws Exception {
        RyaStatementWritable rsw1 = new RyaStatementWritable(rs1, ryaContext);
        RyaStatementWritable rsw1b = new RyaStatementWritable(rs1b, null);
        RyaStatementWritable rsw2 = new RyaStatementWritable(rs2, null);
        RyaStatementWritable rswGraph = new RyaStatementWritable(rsCv, ryaContext);
        RyaStatementWritable rswCv = new RyaStatementWritable(rsCv, ryaContext);
        RyaStatementWritable rswQualifier = new RyaStatementWritable(rsQualifier, ryaContext);
        RyaStatementWritable rswTimestamp = new RyaStatementWritable(rsTimestamp, ryaContext);
        Assert.assertEquals("x.compareTo(x) should always return 0", 0, rsw1.compareTo(rsw1));
        Assert.assertEquals("x.compareTo(x') where x and x' are equal should return 0", 0, rsw1.compareTo(rsw1b));
        Assert.assertEquals("x.compareTo(x') where x and x' are equal should return 0", 0, rsw1b.compareTo(rsw1));
        Assert.assertNotEquals("Statements with different named graphs are not equal", 0, rsw1.compareTo(rswGraph));
        Assert.assertNotEquals("Statements with different column visibilities are not equal", 0, rsw1.compareTo(rswCv));
        Assert.assertNotEquals("Statements with different column qualifiers are not equal", 0, rsw1.compareTo(rswQualifier));
        Assert.assertNotEquals("Statements with different timestamps are not equal", 0, rsw1.compareTo(rswTimestamp));
        Assert.assertEquals("compareTo in opposite directions should yield opposite signs",
                Integer.signum(rsw1.compareTo(rsw2))*-1, Integer.signum(rsw2.compareTo(rsw1)));
        // cycles shouldn't be possible; these comparisons can't all be negative or all be positive:
        int x = Integer.signum(rsw1.compareTo(rsw2))
                + Integer.signum(rsw2.compareTo(rsw1b))
                + Integer.signum(rsw1b.compareTo(rsw1));
        Assert.assertNotEquals("compareTo cycle detected", 3, Math.abs(x));
        // compareTo(null) should always throw an exception:
        expected.expect(NullPointerException.class);
        rsw1.compareTo(null);
    }

    @Test
    public void testSerializeDeserialize() throws Exception {
        RyaStatementWritable rsw1 = new RyaStatementWritable(rs1, ryaContext);
        RyaStatementWritable rsw2 = new RyaStatementWritable(rs2, ryaContext);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream bytesOut = new DataOutputStream(bytes);
        rsw1.write(bytesOut);
        rsw2.write(bytesOut);
        DataInputStream bytesIn = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        RyaStatementWritable deserialized = new RyaStatementWritable();
        // Verify initial deserialization:
        deserialized.readFields(bytesIn);
        Assert.assertEquals("Deserialized statement not equal to original", rsw1, deserialized);
        Assert.assertEquals("Deserialized statement has different hash code", rsw1.hashCode(), deserialized.hashCode());
        Assert.assertEquals("original.compareTo(deserialized) should equal 0", 0, rsw1.compareTo(deserialized));
        // Verify that a second read mutates the Writable object into the correct second record:
        RyaStatement deserializedStatement = deserialized.getRyaStatement();
        deserialized.readFields(bytesIn);
        Assert.assertEquals("Deserialized statement not overwritten on second read", rsw2, deserialized);
        // Verify that the internal RyaStatement object is recreated, not overwritten:
        RyaStatement deserializedStatement2 = deserialized.getRyaStatement();
        Assert.assertNotSame("Reading a second record should create a new internal RyaStatement",
                deserializedStatement, deserializedStatement2);
    }
}