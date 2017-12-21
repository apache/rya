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
package org.apache.rya.mongodb.aggregation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.ListBindingSet;

import com.google.common.collect.Sets;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;

public class PipelineResultIterationTest {
    ValueFactory VF = ValueFactoryImpl.getInstance();

    @SuppressWarnings("unchecked")
    private AggregateIterable<Document> documentIterator(Document ... documents) {
        Iterator<Document> docIter = Arrays.asList(documents).iterator();
        MongoCursor<Document> cursor = Mockito.mock(MongoCursor.class);
        Mockito.when(cursor.hasNext()).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return docIter.hasNext();
            }
        });
        Mockito.when(cursor.next()).thenAnswer(new Answer<Document>() {
            @Override
            public Document answer(InvocationOnMock invocation) throws Throwable {
                return docIter.next();
            }
        });
        AggregateIterable<Document> aggIter = Mockito.mock(AggregateIterable.class);
        Mockito.when(aggIter.iterator()).thenReturn(cursor);
        return aggIter;
    }

    @Test
    public void testIteration() throws QueryEvaluationException {
        HashMap<String, String> nameMap = new HashMap<>();
        nameMap.put("bName", "b");
        nameMap.put("eName", "e");
        PipelineResultIteration iter = new PipelineResultIteration(
                documentIterator(
                        new Document("<VALUES>", new Document("a", "urn:Alice").append("b", "urn:Bob")),
                        new Document("<VALUES>", new Document("a", "urn:Alice").append("b", "urn:Beth")),
                        new Document("<VALUES>", new Document("a", "urn:Alice").append("bName", "urn:Bob")),
                        new Document("<VALUES>", new Document("a", "urn:Alice").append("c", "urn:Carol")),
                        new Document("<VALUES>", new Document("cName", "urn:Carol").append("d", "urn:Dan"))),
                nameMap,
                new QueryBindingSet());
        Assert.assertTrue(iter.hasNext());
        BindingSet bs = iter.next();
        Assert.assertEquals(Sets.newHashSet("a", "b"), bs.getBindingNames());
        Assert.assertEquals("urn:Alice", bs.getBinding("a").getValue().stringValue());
        Assert.assertEquals("urn:Bob", bs.getBinding("b").getValue().stringValue());
        Assert.assertTrue(iter.hasNext());
        bs = iter.next();
        Assert.assertEquals(Sets.newHashSet("a", "b"), bs.getBindingNames());
        Assert.assertEquals("urn:Alice", bs.getBinding("a").getValue().stringValue());
        Assert.assertEquals("urn:Beth", bs.getBinding("b").getValue().stringValue());
        Assert.assertTrue(iter.hasNext());
        bs = iter.next();
        Assert.assertEquals(Sets.newHashSet("a", "b"), bs.getBindingNames());
        Assert.assertEquals("urn:Alice", bs.getBinding("a").getValue().stringValue());
        Assert.assertEquals("urn:Bob", bs.getBinding("b").getValue().stringValue());
        Assert.assertTrue(iter.hasNext());
        bs = iter.next();
        Assert.assertEquals(Sets.newHashSet("a", "c"), bs.getBindingNames());
        Assert.assertEquals("urn:Alice", bs.getBinding("a").getValue().stringValue());
        Assert.assertEquals("urn:Carol", bs.getBinding("c").getValue().stringValue());
        bs = iter.next();
        Assert.assertEquals(Sets.newHashSet("cName", "d"), bs.getBindingNames());
        Assert.assertEquals("urn:Carol", bs.getBinding("cName").getValue().stringValue());
        Assert.assertEquals("urn:Dan", bs.getBinding("d").getValue().stringValue());
        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testIterationGivenBindingSet() throws QueryEvaluationException {
        BindingSet solution = new ListBindingSet(Arrays.asList("b", "c"),
                VF.createURI("urn:Bob"), VF.createURI("urn:Charlie"));
        HashMap<String, String> nameMap = new HashMap<>();
        nameMap.put("bName", "b");
        nameMap.put("cName", "c");
        nameMap.put("c", "cName");
        PipelineResultIteration iter = new PipelineResultIteration(
                documentIterator(
                        new Document("<VALUES>", new Document("a", "urn:Alice").append("b", "urn:Bob")),
                        new Document("<VALUES>", new Document("a", "urn:Alice").append("b", "urn:Beth")),
                        new Document("<VALUES>", new Document("a", "urn:Alice").append("bName", "urn:Bob")),
                        new Document("<VALUES>", new Document("a", "urn:Alice").append("bName", "urn:Beth")),
                        new Document("<VALUES>", new Document("a", "urn:Alice").append("cName", "urn:Carol")),
                        new Document("<VALUES>", new Document("c", "urn:Carol").append("d", "urn:Dan"))),
                nameMap,
                solution);
        Assert.assertTrue(iter.hasNext());
        BindingSet bs = iter.next();
        // Add 'c=Charlie' to first result ('b=Bob' matches)
        Assert.assertEquals(Sets.newHashSet("a", "b", "c"), bs.getBindingNames());
        Assert.assertEquals("urn:Alice", bs.getBinding("a").getValue().stringValue());
        Assert.assertEquals("urn:Bob", bs.getBinding("b").getValue().stringValue());
        Assert.assertEquals("urn:Charlie", bs.getBinding("c").getValue().stringValue());
        Assert.assertTrue(iter.hasNext());
        bs = iter.next();
        // Skip second result ('b=Beth' incompatible with 'b=Bob')
        // Add 'c=Charlie' to third result ('bName=Bob' resolves to 'b=Bob', matches)
        Assert.assertEquals(Sets.newHashSet("a", "b", "c"), bs.getBindingNames());
        Assert.assertEquals("urn:Alice", bs.getBinding("a").getValue().stringValue());
        Assert.assertEquals("urn:Bob", bs.getBinding("b").getValue().stringValue());
        Assert.assertEquals("urn:Charlie", bs.getBinding("c").getValue().stringValue());
        Assert.assertTrue(iter.hasNext());
        bs = iter.next();
        // Skip fourth result ('bName=Beth' resolves to 'b=Beth', incompatible)
        // Skip fifth result ('cName=Carol' resolves to 'c=Carol', incompatible with 'c=Charlie')
        // Add 'b=Bob' and 'c=Charlie' to sixth result ('c=Carol' resolves to 'cName=Carol', compatible)
        Assert.assertEquals(Sets.newHashSet("b", "c", "cName", "d"), bs.getBindingNames());
        Assert.assertEquals("urn:Bob", bs.getBinding("b").getValue().stringValue());
        Assert.assertEquals("urn:Charlie", bs.getBinding("c").getValue().stringValue());
        Assert.assertEquals("urn:Carol", bs.getBinding("cName").getValue().stringValue());
        Assert.assertEquals("urn:Dan", bs.getBinding("d").getValue().stringValue());
        Assert.assertFalse(iter.hasNext());
    }
}
