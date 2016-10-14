package org.apache.rya.indexing.IndexPlanValidator;

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


import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class TupleReArrangerTest {

    @Test
    public void tupleReArrangeTest1() throws MalformedQueryException {

        String queryString = ""//
                + "SELECT ?a ?b ?c ?d ?e" //
                + "{" //
                + "{ ?a a ?b .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?c  }"//
                + " UNION { ?a <uri:talksTo> ?d .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?e  }"//
                + "}";//

        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(queryString, null);
        List<TupleExpr> tuples = TupleReArranger.getTupleReOrderings(pq.getTupleExpr());

        Assert.assertEquals(4, tuples.size());

    }



    @Test
    public void tupleReArrangeTest2() throws MalformedQueryException {

        String queryString = ""//
                + "SELECT ?a ?b ?c ?d ?e ?x ?y" //
                + "{" //
                + " ?e <uri:laughsAt> ?x ." //
                + " ?e <uri:livesIn> ?y . "//
                + "{ ?a a ?b .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?c  }"//
                + " UNION { ?a <uri:talksTo> ?d .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?e  }"//
                + "}";//

        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(queryString, null);
        List<TupleExpr> tuples = TupleReArranger.getTupleReOrderings(pq.getTupleExpr());


        Assert.assertEquals(24, tuples.size());

    }





    @Test
    public void tupleReArrangeTest3() throws MalformedQueryException {

        String queryString = ""//
                + "SELECT ?a ?b ?c ?d ?e ?x ?y" //
                + "{" //
                + " Filter(?c = <uri:label2>)" //
                + " Filter(?x = <uri:somethingFunny>) "//
                + " ?e <uri:laughsAt> ?x ." //
                + " ?e <uri:livesIn> ?y . "//
                + "{ ?a a ?b .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?c  }"//
                + " UNION { ?a <uri:talksTo> ?d .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?e  }"//
                + "}";//

        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(queryString, null);
        List<TupleExpr> tuples = TupleReArranger.getTupleReOrderings(pq.getTupleExpr());

        Assert.assertEquals(24, tuples.size());

    }







    @Test
    public void tupleReArrangeTest4() throws MalformedQueryException {

        String queryString = ""//
                + "SELECT ?a ?b ?c ?d ?e ?x ?y" //
                + "{" //
                + " Filter(?c = <uri:label2>)" //
                + " Filter(?x = <uri:somethingFunny>) "//
                + " Filter(?d = <uri:Fred> ) " //
                + " ?e <uri:laughsAt> ?x ." //
                + " ?e <uri:livesIn> ?y . "//
                + "{ ?a a ?b .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?c  }"//
                + " UNION { ?a <uri:talksTo> ?d .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?e  }"//
                + "}";//

        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(queryString, null);
        TupleExpr te = pq.getTupleExpr();
        new FilterOptimizer().optimize(te, null, null);
        System.out.println(te);
        List<TupleExpr> tuples = TupleReArranger.getTupleReOrderings(te);
        System.out.println(tuples);

        Assert.assertEquals(24, tuples.size());

    }






}
