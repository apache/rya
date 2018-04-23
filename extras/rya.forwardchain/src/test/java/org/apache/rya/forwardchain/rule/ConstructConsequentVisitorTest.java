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

import java.util.Arrays;
import java.util.Set;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.BNodeGenerator;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.MultiProjection;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class ConstructConsequentVisitorTest {
    private static Var s(Value val) {
        return new Var("subject", val);
    }
    private static Var p(Value val) {
        return new Var("predicate", val);
    }
    private static Var o(Value val) {
        return new Var("object", val);
    }
    private static Var anon(Var var) {
        var.setAnonymous(true);
        return var;
    }

    @Test
    public void testGenericSP() {
        Extension extension = new Extension(new SingletonSet(),
                new ExtensionElem(new Var("z"), "z"));
        Projection projection = new Projection(extension, new ProjectionElemList(
                new ProjectionElem("x", "subject"),
                new ProjectionElem("y", "predicate"),
                new ProjectionElem("z", "object")));
        ConstructConsequentVisitor visitor = new ConstructConsequentVisitor();
        projection.visit(visitor);
        Set<StatementPattern> expected = Sets.newHashSet(
                new StatementPattern(s(null), p(null), o(null)));
        Assert.assertEquals(expected, visitor.getConsequents());
    }

    @Test
    public void testConcreteSP() {
        Extension extension = new Extension(new SingletonSet(),
                new ExtensionElem(new ValueConstant(FOAF.PERSON), "x"),
                new ExtensionElem(new ValueConstant(RDF.TYPE), "y"),
                new ExtensionElem(new ValueConstant(OWL.CLASS), "z"));
        Projection projection = new Projection(extension, new ProjectionElemList(
                new ProjectionElem("x", "subject"),
                new ProjectionElem("y", "predicate"),
                new ProjectionElem("z", "object")));
        ConstructConsequentVisitor visitor = new ConstructConsequentVisitor();
        projection.visit(visitor);
        Set<StatementPattern> expected = Sets.newHashSet(
                new StatementPattern(s(FOAF.PERSON), p(RDF.TYPE), o(OWL.CLASS)));
        Assert.assertEquals(expected, visitor.getConsequents());
    }

    @Test
    public void testMissingVariables() {
        Extension extension = new Extension(new SingletonSet(),
                new ExtensionElem(new ValueConstant(FOAF.PERSON), "x"),
                new ExtensionElem(new ValueConstant(RDF.TYPE), "y"));
        Projection projection = new Projection(extension, new ProjectionElemList(
                new ProjectionElem("x", "s"),
                new ProjectionElem("y", "predicate"),
                new ProjectionElem("z", "object")));
        ConstructConsequentVisitor visitor = new ConstructConsequentVisitor();
        projection.visit(visitor);
        Set<StatementPattern> expected = Sets.newHashSet(
                new StatementPattern(s(null), p(RDF.TYPE), o(null)));
        Assert.assertEquals(expected, visitor.getConsequents());
    }

    @Test
    public void testMultiProjection() {
        Extension extension = new Extension(new SingletonSet(),
                new ExtensionElem(new ValueConstant(RDF.TYPE), "rdftype"),
                new ExtensionElem(new ValueConstant(OWL.OBJECTPROPERTY), "owlprop"),
                new ExtensionElem(new ValueConstant(OWL.EQUIVALENTCLASS), "owleqcls"),
                new ExtensionElem(new ValueConstant(OWL.CLASS), "owlclass"));
        MultiProjection projection = new MultiProjection(extension, Arrays.asList(
                new ProjectionElemList(
                        new ProjectionElem("cls", "subject"),
                        new ProjectionElem("rdftype", "predicate"),
                        new ProjectionElem("owlclass", "object")),
                new ProjectionElemList(
                        new ProjectionElem("prop", "subject"),
                        new ProjectionElem("rdftype", "predicate"),
                        new ProjectionElem("owlprop", "object")),
                new ProjectionElemList(
                        new ProjectionElem("owleqcls", "predicate"),
                        new ProjectionElem("cls", "object"))));
        ConstructConsequentVisitor visitor = new ConstructConsequentVisitor();
        projection.visit(visitor);
        Set<StatementPattern> expected = Sets.newHashSet(
                new StatementPattern(s(null), p(RDF.TYPE), o(OWL.CLASS)),
                new StatementPattern(s(null), p(RDF.TYPE), o(OWL.OBJECTPROPERTY)),
                new StatementPattern(s(null), p(OWL.EQUIVALENTCLASS), o(null)));
        Assert.assertEquals(expected, visitor.getConsequents());
    }

    @Test
    public void testNoExtension() {
        StatementPattern sp = new StatementPattern(new Var("x"), new Var("y"), new Var("z"));
        Projection projection = new Projection(sp, new ProjectionElemList(
                new ProjectionElem("x", "subject"),
                new ProjectionElem("y", "predicate"),
                new ProjectionElem("z", "object")));
        ConstructConsequentVisitor visitor = new ConstructConsequentVisitor();
        projection.visit(visitor);
        Set<StatementPattern> expected = Sets.newHashSet(
                new StatementPattern(s(null), p(null), o(null)));
        Assert.assertEquals(expected, visitor.getConsequents());
    }

    @Test
    public void testBNode() {
        Extension extension = new Extension(new SingletonSet(),
                new ExtensionElem(new Var("x"), "x"),
                new ExtensionElem(new BNodeGenerator(), "z"));
        Projection projection = new Projection(extension, new ProjectionElemList(
                new ProjectionElem("x", "subject"),
                new ProjectionElem("y", "predicate"),
                new ProjectionElem("z", "object")));
        ConstructConsequentVisitor visitor = new ConstructConsequentVisitor();
        projection.visit(visitor);
        Set<StatementPattern> expected = Sets.newHashSet(
                new StatementPattern(s(null), p(null), anon(o(null))));
        Assert.assertEquals(expected, visitor.getConsequents());
    }
}
