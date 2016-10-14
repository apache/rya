package org.apache.rya.reasoning;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDFS;

public class ReasonerFactTest {
    static final String wordnet = "http://www.w3.org/2006/03/wn/wn20/instances/";
    static final String wnSchema = "http://www.w3.org/2006/03/wn/wn20/schema/";
    static final URI[] nodes = {
        TestUtils.uri(wordnet, "synset-entity-noun-1"),
        TestUtils.uri(wordnet, "synset-physical_entity-noun-1"),
        TestUtils.uri(wordnet, "synset-object-noun-1"),
        TestUtils.uri(wordnet, "synset-whole-noun-2"),
        TestUtils.uri(wordnet, "synset-living_thing-noun-1"),
        TestUtils.uri(wordnet, "synset-organism-noun-1"),
        TestUtils.uri(wordnet, "synset-person-noun-1"),
        TestUtils.uri(wordnet, "synset-engineer-noun-1"),
        TestUtils.uri(wordnet, "synset-programmer-noun-1")
    };
    static final URI hyper = TestUtils.uri(wnSchema, "hypernymOf");
    static final Schema schema = new Schema();
    static final ArrayList<ArrayList<Fact>> hierarchy = new ArrayList<>();
    static final int MAX_LEVEL = 3;

    static Fact connect(int i, int j) {
        return new Fact(nodes[i], hyper, nodes[j]);
    }

    static Fact connectTransitive(int i, int j, int source, int t) {
        return new Fact(nodes[i], hyper, nodes[j], t, OwlRule.PRP_TRP,
            nodes[source]);
    }

    @Before
    public void buildHierarchy() {
        hierarchy.add(new ArrayList<Fact>());
        int max = 8;
        for (int i = 0; i < max; i++) {
            hierarchy.get(0).add(connect(i, i+1));
        }
        for (int level = 1; level <= MAX_LEVEL; level++) {
            max = max / 2;
            hierarchy.add(new ArrayList<Fact>());
            int d = (int) Math.pow(2, level);
            for (int i = 0; i < max; i++) {
                Fact fact = connectTransitive(i*d, (i*d)+d, (i*d)+(d/2), level);
                ArrayList<Fact> previousLevel = hierarchy.get(level-1);
                Fact sourceA = previousLevel.get(i*2);
                Fact sourceB = previousLevel.get((i*2)+1);
                fact.addSource(sourceA);
                fact.addSource(sourceB);
                hierarchy.get(level).add(fact);
            }
        }
    }

    @Test
    public void testBaseSpan() {
        Assert.assertEquals("Input triple should have span=1",
            1, hierarchy.get(0).get(0).span());
    }

    @Test
    public void testSpan() {
        Assert.assertEquals("Iteration 1 triple should have span=2",
            2, hierarchy.get(1).get(0).span());
    }

    @Test
    public void testMultilevelSpan() {
        Assert.assertEquals("Iteration 3 triple should have span=8",
            8, hierarchy.get(3).get(0).span());
    }

    @Test
    public void testSourceNodes() {
        Assert.assertTrue("Transitive link 0 to 4 should have source nodes 1-3",
            hierarchy.get(2).get(0).derivation.sourceNodes.contains(nodes[1]) &&
            hierarchy.get(2).get(0).derivation.sourceNodes.contains(nodes[2]) &&
            hierarchy.get(2).get(0).derivation.sourceNodes.contains(nodes[3]) &&
            hierarchy.get(2).get(0).derivation.sourceNodes.size() == 3);
    }

    @Test
    public void testTripleEquality() {
        Fact a = hierarchy.get(2).get(0);
        String s = a.getSubject().stringValue();
        String p = a.getPredicate().stringValue();
        String o = a.getObject().stringValue();
        Fact b = new Fact(TestUtils.uri("", s),
            TestUtils.uri("", p), TestUtils.uri("", o));
        Assert.assertEquals("Triple equality should be based on (s, p, o) alone", a, b);
    }

    @Test
    public void testTripleHashCode() {
        Fact a = hierarchy.get(2).get(0);
        String s = a.getSubject().stringValue();
        String p = a.getPredicate().stringValue();
        String o = a.getObject().stringValue();
        Fact b = new Fact(TestUtils.uri("", s),
            TestUtils.uri("", p), TestUtils.uri("", o));
        Assert.assertEquals("hashCode should be based on (s, p, o) alone",
            a.hashCode(), b.hashCode());
    }

    @Test
    public void testTripleInequality() {
        Fact a = hierarchy.get(2).get(0);
        Fact b = a.clone();
        Statement stmt = new StatementImpl(TestUtils.uri(a.getSubject().stringValue()),
            a.getPredicate(), a.getObject()); // subject will have extra prefix
        b.setTriple(stmt);
        Assert.assertFalse("Triple equality should be based on (s, p, o)", a.equals(b));
    }

    @Test
    public void testClone() {
        Fact a = hierarchy.get(2).get(0);
        Fact b = a.clone();
        Assert.assertEquals("clone should equal() original", a, b);
        Assert.assertEquals("clone.subject should be equal to original",
            a.getSubject(), b.getSubject());
        Assert.assertEquals("clone.predicate should be equal to original",
            a.getPredicate(), b.getPredicate());
        Assert.assertEquals("clone.object should be equal to original",
            a.getObject(), b.getObject());
        Assert.assertEquals("clone.derivation should be equal to original",
            a.getDerivation(), b.getDerivation());
        Assert.assertEquals("clone.useful should be equal to original",
            a.isUseful(), b.isUseful());
        Assert.assertEquals("clone.span() should equal original",
            a.span(), b.span());
    }

    @Test
    public void testSerializeDeserialize() throws Exception {
        Fact a = hierarchy.get(2).get(0);
        Fact b = new Fact();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        a.write(new DataOutputStream(bytes));
        b.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assert.assertEquals("deserialized triple should equal() original", a, b);
        Assert.assertEquals("deserialized subject should be equal to original",
            a.getSubject(), b.getSubject());
        Assert.assertEquals("deserialized predicate should be equal to original",
            a.getPredicate(), b.getPredicate());
        Assert.assertEquals("deserialized object should be equal to original",
            a.getObject(), b.getObject());
        Assert.assertEquals("deserialized derivation should be equal to original",
            a.getDerivation(), b.getDerivation());
        Assert.assertEquals("deserialized useful should be equal to original",
            a.isUseful(), b.isUseful());
        Assert.assertEquals("deserialized span() should equal original", a.span(), b.span());
    }

    @Test
    public void testSerializeDeserializeString() throws Exception {
        Fact a = TestUtils.fact(hyper, TestUtils.uri(wnSchema, "gloss"),
            TestUtils.stringLiteral("(a word that is more generic than a given word)"));
        Fact b = new Fact();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        a.write(new DataOutputStream(bytes));
        b.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assert.assertEquals("deserialized triple should equal() original", a, b);
        Assert.assertEquals("deserialized subject should be equal to original",
            a.getSubject(), b.getSubject());
        Assert.assertEquals("deserialized predicate should be equal to original",
            a.getPredicate(), b.getPredicate());
        Assert.assertEquals("deserialized object should be equal to original",
            a.getObject(), b.getObject());
        Assert.assertEquals("deserialized derivation should be equal to original",
            a.getDerivation(), b.getDerivation());
        Assert.assertEquals("deserialized useful should be equal to original",
            a.isUseful(), b.isUseful());
        Assert.assertEquals("deserialized span() should equal original", a.span(), b.span());
    }

    @Test
    public void testSerializeDeserializeLanguage() throws Exception {
        Fact a = TestUtils.fact(hyper, TestUtils.uri(wnSchema, "gloss"),
            TestUtils.stringLiteral("(a word that is more generic than a given word)", "e"));
        Fact b = new Fact();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        a.write(new DataOutputStream(bytes));
        b.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assert.assertEquals("deserialized triple should equal() original", a, b);
        Assert.assertEquals("deserialized subject should be equal to original",
            a.getSubject(), b.getSubject());
        Assert.assertEquals("deserialized predicate should be equal to original",
            a.getPredicate(), b.getPredicate());
        Assert.assertEquals("deserialized object should be equal to original",
            a.getObject(), b.getObject());
        Assert.assertEquals("deserialized derivation should be equal to original",
            a.getDerivation(), b.getDerivation());
        Assert.assertEquals("deserialized useful should be equal to original",
            a.isUseful(), b.isUseful());
        Assert.assertEquals("deserialized span() should equal original", a.span(), b.span());
    }

    @Test
    public void testSerializeDeserializeInt() throws Exception {
        Fact a = TestUtils.fact(TestUtils.uri(wnSchema, "inSynset"),
            OWL.MAXCARDINALITY,
            TestUtils.intLiteral("1"));
        Fact b = new Fact();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        a.write(new DataOutputStream(bytes));
        b.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assert.assertEquals("deserialized triple should equal() original", a, b);
        Assert.assertEquals("deserialized subject should be equal to original",
            a.getSubject(), b.getSubject());
        Assert.assertEquals("deserialized predicate should be equal to original",
            a.getPredicate(), b.getPredicate());
        Assert.assertEquals("deserialized object should be equal to original",
            a.getObject(), b.getObject());
        Assert.assertEquals("deserialized derivation should be equal to original",
            a.getDerivation(), b.getDerivation());
        Assert.assertEquals("deserialized useful should be equal to original",
            a.isUseful(), b.isUseful());
        Assert.assertEquals("deserialized span() should equal original", a.span(), b.span());
    }

    @Test
    public void testSerializeDeserializeBnode() throws Exception {
        Fact a = TestUtils.fact(TestUtils.bnode("foo"),
            RDFS.SUBCLASSOF, TestUtils.bnode("bar"));
        Fact b = new Fact();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        a.write(new DataOutputStream(bytes));
        b.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assert.assertEquals("deserialized triple should equal() original", a, b);
        Assert.assertEquals("deserialized subject should be equal to original",
            a.getSubject(), b.getSubject());
        Assert.assertEquals("deserialized predicate should be equal to original",
            a.getPredicate(), b.getPredicate());
        Assert.assertEquals("deserialized object should be equal to original",
            a.getObject(), b.getObject());
        Assert.assertEquals("deserialized derivation should be equal to original",
            a.getDerivation(), b.getDerivation());
        Assert.assertEquals("deserialized useful should be equal to original",
            a.isUseful(), b.isUseful());
        Assert.assertEquals("deserialized span() should equal original", a.span(), b.span());
    }
}
