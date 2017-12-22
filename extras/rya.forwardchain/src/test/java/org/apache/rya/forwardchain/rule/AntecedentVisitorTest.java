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
package org.apache.rya.forwardchain.rule;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Sets;

public class AntecedentVisitorTest {
    private static Var c(Value val) {
        Var v = new Var("-const-" + val.stringValue(), val);
        v.setAnonymous(true);
        return v;
    }

    private static ValueFactory VF = ValueFactoryImpl.getInstance();
    private static String EX = "http://example.org/";
    private static URI G1 = VF.createURI(EX, "Graph1");
    private static URI G2 = VF.createURI(EX, "Graph2");

    @Test
    public void testSelectQuery() throws Exception {
        String text = "PREFIX foaf: <" + FOAF.NAMESPACE + ">\n"
                + "SELECT * WHERE {\n"
                + "  ?x a foaf:Person .\n"
                + "  ?y a foaf:Person .\n"
                + "  ?x foaf:knows ?y .\n"
                + "}";
        ParsedQuery query = new SPARQLParser().parseQuery(text, null);
        AntecedentVisitor visitor = new AntecedentVisitor();
        query.getTupleExpr().visit(visitor);
        Set<StatementPattern> expected = Sets.newHashSet(
                new StatementPattern(new Var("x"), c(RDF.TYPE), c(FOAF.PERSON)),
                new StatementPattern(new Var("y"), c(RDF.TYPE), c(FOAF.PERSON)),
                new StatementPattern(new Var("x"), c(FOAF.KNOWS), new Var("y")));
        Assert.assertEquals(expected, visitor.getAntecedents());
    }

    @Test
    public void testConstructQuery() throws Exception {
        String text = "PREFIX foaf: <" + FOAF.NAMESPACE + ">\n"
                + "CONSTRUCT {\n"
                + "  ?y foaf:knows ?x .\n"
                + "  ?y <urn:knows> ?x .\n"
                + "  ?x <urn:knows> ?y .\n"
                + "} WHERE {\n"
                + "  ?x a foaf:Person .\n"
                + "  ?y a foaf:Person .\n"
                + "  ?x foaf:knows ?y .\n"
                + "}";
        ParsedQuery query = new SPARQLParser().parseQuery(text, null);
        AntecedentVisitor visitor = new AntecedentVisitor();
        query.getTupleExpr().visit(visitor);
        Set<StatementPattern> expected = Sets.newHashSet(
                new StatementPattern(new Var("x"), c(RDF.TYPE), c(FOAF.PERSON)),
                new StatementPattern(new Var("y"), c(RDF.TYPE), c(FOAF.PERSON)),
                new StatementPattern(new Var("x"), c(FOAF.KNOWS), new Var("y")));
        Assert.assertEquals(expected, visitor.getAntecedents());
    }

    @Test
    public void testComplexQuery() throws Exception {
        String text = "PREFIX foaf: <" + FOAF.NAMESPACE + ">\n"
                + "PREFIX ex: <" + EX + ">\n"
                + "SELECT * WHERE {\n"
                + "  { ?x a foaf:Person } UNION {\n"
                + "    GRAPH ex:Graph1 { ?y a foaf:Person }\n"
                + "  } .\n"
                + "  GRAPH ex:Graph2 {\n"
                + "    ?x foaf:knows ?y .\n"
                + "  }\n ."
                + "  OPTIONAL { ?x foaf:mbox ?m } .\n"
                + "  FILTER (?x != ?y) .\n"
                + "}";
        ParsedQuery query = new SPARQLParser().parseQuery(text, null);
        AntecedentVisitor visitor = new AntecedentVisitor();
        query.getTupleExpr().visit(visitor);
        Set<StatementPattern> expected = Sets.newHashSet(
                new StatementPattern(Scope.NAMED_CONTEXTS, new Var("y"), c(RDF.TYPE), c(FOAF.PERSON), c(G1)),
                new StatementPattern(new Var("x"), c(RDF.TYPE), c(FOAF.PERSON)),
                new StatementPattern(Scope.NAMED_CONTEXTS, new Var("x"), c(FOAF.KNOWS), new Var("y"), c(G2)),
                new StatementPattern(new Var("x"), c(FOAF.MBOX), new Var("m")));
        Assert.assertEquals(expected, visitor.getAntecedents());
    }

    @Test
    public void testBNodeQuery() throws Exception {
        String text = "PREFIX foaf: <" + FOAF.NAMESPACE + ">\n"
                + "SELECT * WHERE {\n"
                + "  ?x a [ rdfs:subClassOf foaf:Person ] .\n"
                + "  ?x foaf:knows ?y .\n"
                + "}";
        ParsedQuery query = new SPARQLParser().parseQuery(text, null);
        AntecedentVisitor visitor = new AntecedentVisitor();
        query.getTupleExpr().visit(visitor);
        Set<StatementPattern> actual = visitor.getAntecedents();
        Assert.assertEquals(3, actual.size());
        StatementPattern knows = new StatementPattern(new Var("x"), c(FOAF.KNOWS), new Var("y"));
        Assert.assertTrue(actual.remove(knows));
        Assert.assertTrue(actual.removeIf(sp -> {
            return sp.getSubjectVar().equals(new Var("x"))
                    && RDF.TYPE.equals(sp.getPredicateVar().getValue())
                    && sp.getObjectVar().getValue() == null;
        }));
        Assert.assertTrue(actual.removeIf(sp -> {
            return sp.getSubjectVar().getValue() == null
                    && RDFS.SUBCLASSOF.equals(sp.getPredicateVar().getValue())
                    && FOAF.PERSON.equals(sp.getObjectVar().getValue());
        }));
    }

    @Test
    public void testNoSP() throws Exception {
        String text = "CONSTRUCT {\n"
                + "  owl:Thing a owl:Class ."
                + "  owl:Nothing a owl:Class ."
                + "  owl:Nothing rdfs:subClassOf owl:Thing ."
                + "} WHERE { }";
        ParsedQuery query = new SPARQLParser().parseQuery(text, null);
        AntecedentVisitor visitor = new AntecedentVisitor();
        query.getTupleExpr().visit(visitor);
        Set<StatementPattern> expected = Sets.newHashSet();
        Assert.assertEquals(expected, visitor.getAntecedents());
    }
}
