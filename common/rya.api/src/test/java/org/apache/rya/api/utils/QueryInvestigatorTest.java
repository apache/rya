/**
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
package org.apache.rya.api.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.junit.Test;

/**
 * Unit tests the methods of {@link QueryInvestigator}.
 */
public class QueryInvestigatorTest {

    @Test
    public void isConstruct_true() throws Exception {
        final String sparql =
                "PREFIX vCard: <http://www.w3.org/2001/vcard-rdf/3.0#> " +
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                "CONSTRUCT { " +
                    "?X vCard:FN ?name . " +
                    "?X vCard:URL ?url . " +
                    "?X vCard:TITLE ?title . " +
                "} " +
                "FROM <http://www.w3.org/People/Berners-Lee/card> " +
                "WHERE { " +
                    "OPTIONAL { ?X foaf:name ?name . FILTER isLiteral(?name) . } " +
                    "OPTIONAL { ?X foaf:homepage ?url . FILTER isURI(?url) . } " +
                    "OPTIONAL { ?X foaf:title ?title . FILTER isLiteral(?title) . } " +
                "}";

        assertTrue( QueryInvestigator.isConstruct(sparql) );
    }

    @Test
    public void isConstruct_false_notAConstruct() throws Exception {
        final String sparql = "SELECT * WHERE { ?a ?b ?c . }";
        assertFalse( QueryInvestigator.isConstruct(sparql) );
    }

    @Test
    public void isConstruct_false_notAConstructWithKeywords() throws Exception {
        final String sparql =
                "SELECT ?construct " +
                "WHERE {" +
                "   ?construct <urn:built> <urn:skyscraper> ." +
                "}";
        assertFalse( QueryInvestigator.isConstruct(sparql) );
    }

    @Test
    public void isConstruct_false_notAQuery() throws Exception {
        final String sparql =
                "PREFIX Sensor: <http://example.com/Equipment.owl#> " +
                "INSERT { " +
                    "?subject Sensor:test2 ?newValue " +
                "} WHERE {" +
                    "values (?oldValue ?newValue) {" +
                        "('testValue1' 'newValue1')" +
                        "('testValue2' 'newValue2')" +
                    "}" +
                    "?subject Sensor:test1 ?oldValue" +
                "}";

        assertFalse( QueryInvestigator.isConstruct(sparql) );
    }

    @Test(expected = MalformedQueryException.class)
    public void isConstruct_false_malformed() throws MalformedQueryException {
        assertFalse( QueryInvestigator.isConstruct("not sparql") );
    }

    @Test
    public void isInsert_true() throws Exception {
        final String sparql =
                "PREFIX Sensor: <http://example.com/Equipment.owl#> " +
                "INSERT { " +
                    "?subject Sensor:test2 ?newValue " +
                "} WHERE {" +
                    "values (?oldValue ?newValue) {" +
                        "('testValue1' 'newValue1')" +
                        "('testValue2' 'newValue2')" +
                    "}" +
                    "?subject Sensor:test1 ?oldValue" +
                "}";

        assertTrue( QueryInvestigator.isInsertWhere(sparql) );
    }

    @Test
    public void isInsert_false_notAnInsert() throws Exception {
        final String sparql =
                "PREFIX dc:  <http://purl.org/dc/elements/1.1/> " +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                "DELETE { " +
                    "?book ?p ?v " +
                "} WHERE { " +
                    "?book dc:date ?date . " +
                    "FILTER ( ?date < \"2000-01-01T00:00:00\"^^xsd:dateTime ) " +
                    "?book ?p ?v " +
                "}";

        assertFalse( QueryInvestigator.isInsertWhere(sparql) );
    }

    @Test
    public void isInsert_false_notAnInsertWithKeywords() throws Exception {
        final String sparql =
                "DELETE" +
                "{ " +
                "    ?bookInsert ?p ?o" +
                "}" +
                "WHERE" +
                "{ " +
                "    ?bookInsert <urn:datePrinted> ?datePrinted  ." +
                "    FILTER ( ?datePrinted < \"2018-01-01T00:00:00\"^^xsd:dateTime )" +
                "    ?bookInsert ?p ?o" +
                "}";

        assertFalse( QueryInvestigator.isInsertWhere(sparql) );
    }

    @Test
    public void isInsert_false_notAnUpdate() throws Exception {
        final String sparql = "SELECT * WHERE { ?a ?b ?c . }";
        assertFalse( QueryInvestigator.isInsertWhere(sparql) );
    }

    @Test(expected = MalformedQueryException.class)
    public void isInsert_false_malformed() throws MalformedQueryException {
        assertFalse( QueryInvestigator.isInsertWhere("not sparql") );
    }
}
