package org.apache.rya;

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
import java.io.IOException;
import java.io.InputStream;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.openrdf.model.Resource;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.text.tsv.SPARQLResultsTSVWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.namespace.NamespaceManager;
import junit.framework.TestCase;

/**
 * The purpose of this is to provide a test case that illustrates a failure that is being encountered. A working test is
 * provided as well to demonstrate that a successful query can be made.
 */
public class ArbitraryLengthQueryTest extends TestCase {

    /**
     * The repository used for the tests.
     */
    private Repository repository;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        final RdfCloudTripleStore store = new MockRdfCloudStore();

        final NamespaceManager nm = new NamespaceManager(store.getRyaDAO(), store.getConf());
        store.setNamespaceManager(nm);

        repository = new RyaSailRepository(store);
        repository.initialize();

        load();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        repository.shutDown();
    }

    /**
     * This test works. The expected result is 6 rows ranging from "Model1Class 1" through "Model1Class 6".
     *
     * @throws RepositoryException
     * @throws QueryEvaluationException
     * @throws TupleQueryResultHandlerException
     *
     * @throws MalformedQueryException
     */
    public void testWithoutSubquery() throws RepositoryException, QueryEvaluationException, TupleQueryResultHandlerException, MalformedQueryException {
        final String query = "SELECT ?i ?i_label ?i_class ?i_v1"
                + "WHERE {"
                + "?i <http://www.w3.org/2000/01/rdf-schema#label> ?i_label ."
                + "?i a ?i_class ."
                + "?i_class <http://www.w3.org/2000/01/rdf-schema#subClassOf>* <http://dragon-research.com/cham/model/model1#Model1Class> ."
                + "OPTIONAL { ?i <http://dragon-research.com/cham/model/model1#name> ?i_v1 } ."
                + "}"
                + "ORDER BY ?i_label";

        final RepositoryConnection conn = repository.getConnection();
        final TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        RdfCloudTripleStoreConnectionTest.CountTupleHandler countTupleHandler = new RdfCloudTripleStoreConnectionTest.CountTupleHandler();
        tupleQuery.evaluate(countTupleHandler);
        assertEquals(6, countTupleHandler.getCount());
        conn.close();
    }

    /**
     * This test fails. The expected result is 6 rows ranging from "Model1Class 1 Event" to "Model1Class 6 Event". The
     * current result is a RejectedExecutionException.
     *
     * @throws RepositoryException
     * @throws QueryEvaluationException
     * @throws TupleQueryResultHandlerException
     *
     * @throws MalformedQueryException
     */
    public void testWithSubquery() throws RepositoryException, QueryEvaluationException, TupleQueryResultHandlerException, MalformedQueryException {
        final String query = "SELECT ?i ?i_label ?i_class ?i_v1 ?i_v2 ?i_v2_label ?i_v2_class ?i_v2_v1"
                + "WHERE {"
                + "?i <http://www.w3.org/2000/01/rdf-schema#label> ?i_label ."
                + "?i a ?i_class ."
                + "?i_class <http://www.w3.org/2000/01/rdf-schema#subClassOf>* <http://dragon-research.com/cham/model/model1#Event> ."
                + "OPTIONAL { ?i <http://dragon-research.com/cham/model/model1#name> ?i_v1 } ."
                + "?i <http://dragon-research.com/cham/model/model1#hasTemporalEntity> ?i_v2 ."
                + "{"
                + "SELECT ?i_v2 ?i_v2_label ?i_v2_class ?i_v2_v1"
                + "WHERE {"
                + "?i_v2 <http://www.w3.org/2000/01/rdf-schema#label> ?i_v2_label ."
                + "?i_v2 a ?i_v2_class ."
                + "?i_v2_class <http://www.w3.org/2000/01/rdf-schema#subClassOf>* <http://dragon-research.com/cham/model/model1#TemporalInstant> ."
                + "OPTIONAL { ?i_v2 <http://dragon-research.com/cham/model/model1#dateTime> ?i_v2_v1 } ."
                + "}"
                + "ORDER BY ?i_v2_label"
                + "}"
                + "}"
                + "ORDER BY ?i_label";

        final RepositoryConnection conn = repository.getConnection();
        final TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        RdfCloudTripleStoreConnectionTest.CountTupleHandler countTupleHandler = new RdfCloudTripleStoreConnectionTest.CountTupleHandler();
        tupleQuery.evaluate(countTupleHandler);
        assertEquals(6, countTupleHandler.getCount());
        conn.close();
    }

    /**
     * Load the t-box and a-box turtle from strings defined within this class.
     *
     * @throws RepositoryException
     * @throws RDFParseException
     * @throws IOException
     */
    private void load() throws RepositoryException, RDFParseException, IOException {
        final RepositoryConnection conn = repository.getConnection();

        // T-Box
        String ttlString = MODEL_TTL;
        InputStream stringInput = new ByteArrayInputStream(ttlString.getBytes());
        conn.add(stringInput, "http://dragon-research.com/cham/model/model1", RDFFormat.TURTLE, new Resource[]{});

        // A-Box
        ttlString = BUCKET_TTL;
        stringInput = new ByteArrayInputStream(ttlString.getBytes());
        conn.add(stringInput, "http://dragon-research.com/cham/bucket/bucket1", RDFFormat.TURTLE, new Resource[]{});

        conn.commit();
        conn.close();
    }

    /**
     * Mock RDF cloud store for one shot testing.
     */
    public class MockRdfCloudStore extends RdfCloudTripleStore {
        public MockRdfCloudStore() {
            super();
            final Instance instance = new MockInstance();
            try {
                final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
                setConf(conf);

                final Connector connector = instance.getConnector("", "");
                final AccumuloRyaDAO cdao = new AccumuloRyaDAO();
                cdao.setConf(conf);
                cdao.setConnector(connector);
                setRyaDAO(cdao);
                inferenceEngine = new InferenceEngine();
                inferenceEngine.setRyaDAO(cdao);
                inferenceEngine.setRefreshGraphSchedule(5000); //every 5 sec
                inferenceEngine.setConf(conf);
                setInferenceEngine(inferenceEngine);
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * The ontology t-box in turtle.
     */
    private static String MODEL_TTL = "@prefix :        <http://dragon-research.com/cham/model/model1#> ."
            + "@prefix cham:    <http://dragon-research.com/cham/schema#> ."
            + "@prefix dc:      <http://purl.org/dc/elements/1.1/> ."
            + "@prefix owl:     <http://www.w3.org/2002/07/owl#> ."
            + "@prefix qudt:    <http://data.nasa.gov/qudt/owl/qudt#> ."
            + "@prefix rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ."
            + "@prefix rdfs:    <http://www.w3.org/2000/01/rdf-schema#> ."
            + "@prefix unit:    <http://data.nasa.gov/qudt/owl/unit#> ."
            + "@prefix xml:     <http://www.w3.org/XML/1998/namespace> ."
            + "@prefix xsd:     <http://www.w3.org/2001/XMLSchema#> ."
            + ""
            + "<http://dragon-research.com/cham/model/model1>"
            + "      rdf:type owl:Ontology ;"
            + "      rdfs:label \"Model1 Ontology\"^^xsd:string ;"
            + "      :versionInfo \"0.1\"^^xsd:string ;"
            + "      dc:title \"Model1 Ontology\"^^xsd:string ."
            + ""
            + ":ModelClassD"
            + "      rdf:type owl:Class ;"
            + "      rdfs:label \"ModelClassD\"^^xsd:string ;"
            + "      rdfs:subClassOf"
            + "              [ rdf:type owl:Restriction ;"
            + "                owl:maxQualifiedCardinality"
            + "                        \"1\"^^xsd:nonNegativeInteger ;"
            + "                owl:onDataRange xsd:string ;"
            + "                owl:onProperty :name"
            + "              ] ;"
            + "      rdfs:subClassOf"
            + "              [ rdf:type owl:Restriction ;"
            + "                owl:allValuesFrom :Model1ClassAssoc ;"
            + "                owl:onProperty :hasModel1ClassAssoc"
            + "              ] ."
            + ""
            + ":ModelClassC"
            + "      rdf:type owl:Class ;"
            + "      rdfs:label \"ModelClassC\"^^xsd:string ;"
            + "      rdfs:subClassOf :ModelClassD ."
            + ""
            + ":Modle1ClassB"
            + "      rdf:type owl:Class ;"
            + "      rdfs:label \"Modle1ClassB\"^^xsd:string ;"
            + "      rdfs:subClassOf :ModelClassC ."
            + ""
            + ":Model1ClassA"
            + "      rdf:type owl:Class ;"
            + "      rdfs:label \"Model1ClassA\"^^xsd:string ;"
            + "      rdfs:subClassOf :Modle1ClassB ."
            + ""
            + ":Model1Class"
            + "      rdf:type owl:Class ;"
            + "      rdfs:label \"Model1Class\"^^xsd:string ;"
            + "      rdfs:subClassOf :Model1ClassA ;"
            + "      rdfs:subClassOf"
            + "              [ rdf:type owl:Restriction ;"
            + "                owl:maxQualifiedCardinality"
            + "                        \"1\"^^xsd:nonNegativeInteger ;"
            + "                owl:onDataRange xsd:string ;"
            + "                owl:onProperty :model1ClassId"
            + "              ] ."
            + ""
            + ":Model1Event"
            + "      rdf:type owl:Class ;"
            + "      rdfs:label \"Model1Event\"^^xsd:string ;"
            + "      rdfs:subClassOf :Event ;"
            + "      rdfs:subClassOf"
            + "              [ rdf:type owl:Restriction ;"
            + "                owl:allValuesFrom :Model1ClassA ;"
            + "                owl:onProperty :hasModel1ClassA"
            + "              ] ."
            + ""
            + ":Model1ClassAssoc"
            + "      rdf:type owl:Class ;"
            + "      rdfs:label \"Model1ClassAssoc\"^^xsd:string ;"
            + "      rdfs:subClassOf owl:Thing ;"
            + "      rdfs:subClassOf"
            + "              [ rdf:type owl:Restriction ;"
            + "                owl:maxQualifiedCardinality"
            + "                        \"1\"^^xsd:nonNegativeInteger ;"
            + "                owl:onDataRange xsd:string ;"
            + "                owl:onProperty :name"
            + "              ] ;"
            + "      rdfs:subClassOf"
            + "              [ rdf:type owl:Restriction ;"
            + "                owl:maxQualifiedCardinality"
            + "                        \"1\"^^xsd:nonNegativeInteger ;"
            + "                owl:onClass :ModelClassD ;"
            + "                owl:onProperty :hasEntity"
            + "              ] ;"
            + "      rdfs:subClassOf"
            + "              [ rdf:type owl:Restriction ;"
            + "                owl:allValuesFrom :ModelClassD ;"
            + "                owl:onProperty :hasEntity"
            + "              ] ."
            + ""
            + ":TemporalEntity"
            + "      rdf:type owl:Class ;"
            + "      rdfs:label \"TemporalEntity\"^^xsd:string ;"
            + "      rdfs:subClassOf owl:Thing ."
            + ""
            + ":TemporalInstant"
            + "      rdf:type owl:Class ;"
            + "      rdfs:label \"TemporalInstant\"^^xsd:string ;"
            + "      rdfs:subClassOf :TemporalEntity ;"
            + "      rdfs:subClassOf"
            + "              [ rdf:type owl:Restriction ;"
            + "                owl:maxQualifiedCardinality"
            + "                        \"1\"^^xsd:nonNegativeInteger ;"
            + "                owl:onDataRange xsd:dateTime ;"
            + "                owl:onProperty :dateTime"
            + "              ] ."
            + ""
            + ":model1ClassId"
            + "      rdf:type owl:DatatypeProperty ;"
            + "      rdfs:domain :Model1Class ;"
            + "      rdfs:label \"model1ClassId\"^^xsd:string ;"
            + "      rdfs:range xsd:string ."
            + ""
            + ":hasModel1ClassAssoc"
            + "      rdf:type owl:ObjectProperty ;"
            + "      rdfs:domain :ModelClassD ;"
            + "      rdfs:label \"hasModel1ClassAssoc\"^^xsd:string ;"
            + "      rdfs:range :Model1ClassAssoc ."
            + ""
            + ":name"
            + "      rdf:type owl:DatatypeProperty ;"
            + "      rdfs:domain :Model1ClassAssoc , :ModelClassD ;"
            + "      rdfs:label \"name\"^^xsd:string ;"
            + "      rdfs:range xsd:string ."
            + ""
            + ":hasTemporalEntity"
            + "      rdf:type owl:ObjectProperty ;"
            + "      rdfs:domain :ThreatAnalysis , :Event , :TrackingData , :Threat , :Vulnerability ;"
            + "      rdfs:label \"hasTemporalEntity\"^^xsd:string ;"
            + "      rdfs:range :TemporalEntity ."
            + ""
            + ":hasEntity"
            + "      rdf:type owl:ObjectProperty ;"
            + "      rdfs:domain :Model1ClassAssoc ;"
            + "      rdfs:label \"hasEntity\"^^xsd:string ;"
            + "      rdfs:range :ModelClassD ."
            + ""
            + ":dateTime"
            + "      rdf:type owl:DatatypeProperty ;"
            + "      rdfs:domain :TemporalInstant ;"
            + "      rdfs:label \"dateTime\"^^xsd:string ;"
            + "      rdfs:range xsd:dateTime ."
            + ""
            + ":Event"
            + "      rdf:type owl:Class ;"
            + "      rdfs:label \"Event\"^^xsd:string ;"
            + "      rdfs:subClassOf :ModelClassD ;"
            + "      rdfs:subClassOf"
            + "              [ rdf:type owl:Restriction ;"
            + "                owl:allValuesFrom :TemporalEntity ;"
            + "                owl:onProperty :hasTemporalEntity"
            + "              ] ;"
            + "      rdfs:subClassOf"
            + "              [ rdf:type owl:Restriction ;"
            + "                owl:maxQualifiedCardinality"
            + "                        \"1\"^^xsd:nonNegativeInteger ;"
            + "                owl:onClass :TemporalEntity ;"
            + "                owl:onProperty :hasTemporalEntity"
            + "              ] ."
            + ""
            + ":hasModel1ClassA"
            + "      rdf:type owl:ObjectProperty ;"
            + "      rdfs:domain :Model1Event ;"
            + "      rdfs:label \"hasModel1ClassA\"^^xsd:string ;"
            + "      rdfs:range :Model1ClassA ."
            + ""
            + "rdfs:label"
            + "      rdf:type owl:AnnotationProperty ."
            + ""
            + "xsd:date"
            + "      rdf:type rdfs:Datatype ."
            + ""
            + "xsd:time"
            + "      rdf:type rdfs:Datatype .";

    /**
     * The ontology a-box in turtle.
     */
    private static String BUCKET_TTL = "@prefix :        <http://dragon-research.com/cham/bucket/bucket1#> ."
            + "@prefix rdfs:    <http://www.w3.org/2000/01/rdf-schema#> ."
            + "@prefix owl:     <http://www.w3.org/2002/07/owl#> ."
            + "@prefix xsd:     <http://www.w3.org/2001/XMLSchema#> ."
            + "@prefix rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ."
            + "@prefix model1:   <http://dragon-research.com/cham/model/model1#> ."
            + ""
            + ":i1   a       model1:Model1Class ;"
            + "      rdfs:label \"Model1Class 1\"^^xsd:string ;"
            + "      model1:name \"Model1Class 1\"^^xsd:string ;"
            + "      model1:hasModel1ClassAssoc :i1-assoc ;"
            + "      model1:model1ClassId \"ID01\"^^xsd:string ."
            + "      "
            + ":i1-assoc a model1:Model1ClassAssoc ;"
            + "      rdfs:label \"Model1Class 1 Assoc\"^^xsd:string ;"
            + "      model1:hasEntity :i1-event ."
            + "      "
            + ":i1-event a model1:Model1Event ;"
            + "      rdfs:label \"Model1Class 1 Event\"^^xsd:string ;"
            + "      model1:hasTemporalEntity :i1-time ."
            + ""
            + ":i1-time a model1:TemporalInstant ;"
            + "      rdfs:label \"Model1Class 1 Time\"^^xsd:string ;"
            + "      model1:dateTime \"1994-02-07T21:47:01.000Z\"^^xsd:dateTime ."
            + "      "
            + ":i2   a       model1:Model1Class ;"
            + "      rdfs:label \"Model1Class 2\"^^xsd:string ;"
            + "      model1:name \"Model1Class 2\"^^xsd:string ;"
            + "      model1:hasModel1ClassAssoc :i2-assoc ;"
            + "      model1:model1ClassId \"ID02\"^^xsd:string ."
            + ""
            + ":i2-assoc a model1:Model1ClassAssoc ;"
            + "      rdfs:label \"Model1Class 2 Assoc\"^^xsd:string ;"
            + "      model1:hasEntity :i2-event ."
            + "      "
            + ":i2-event a model1:Model1Event ;"
            + "      rdfs:label \"Model1Class 2 Event\"^^xsd:string ;"
            + "      model1:hasTemporalEntity :i2-time ."
            + ""
            + ":i2-time a model1:TemporalInstant ;"
            + "      rdfs:label \"Model1Class 2 Time\"^^xsd:string ;"
            + "      model1:dateTime \"1995-11-06T05:15:01.000Z\"^^xsd:dateTime ."
            + "      "
            + ":i3   a       model1:Model1Class ;"
            + "      rdfs:label \"Model1Class 3\"^^xsd:string ;"
            + "      model1:name \"Model1Class 3\"^^xsd:string ;"
            + "      model1:hasModel1ClassAssoc :i3-assoc ;"
            + "      model1:model1ClassId \"ID03\"^^xsd:string ."
            + ""
            + ":i3-assoc a model1:Model1ClassAssoc ;"
            + "      rdfs:label \"Model1Class 3 Assoc\"^^xsd:string ;"
            + "      model1:hasEntity :i3-event ."
            + "      "
            + ":i3-event a model1:Model1Event ;"
            + "      rdfs:label \"Model1Class 3 Event\"^^xsd:string ;"
            + "      model1:hasTemporalEntity :i3-time ."
            + ""
            + ":i3-time a model1:TemporalInstant ;"
            + "      rdfs:label \"Model1Class 3 Time\"^^xsd:string ;"
            + "      model1:dateTime \"1999-04-30T16:30:00.000Z\"^^xsd:dateTime ."
            + "      "
            + ":i4   a       model1:Model1Class ;"
            + "      rdfs:label \"Model1Class 4\"^^xsd:string ;"
            + "      model1:name \"Model1Class 4\"^^xsd:string ;"
            + "      model1:hasModel1ClassAssoc :i4-assoc ;"
            + "      model1:model1ClassId \"ID04\"^^xsd:string ."
            + ""
            + ":i4-assoc a model1:Model1ClassAssoc ;"
            + "      rdfs:label \"Model1Class 4 Assoc\"^^xsd:string ;"
            + "      model1:hasEntity :i4-event ."
            + "      "
            + ":i4-event a model1:Model1Event ;"
            + "      rdfs:label \"Model1Class 4 Event\"^^xsd:string ;"
            + "      model1:hasTemporalEntity :i4-time ."
            + ""
            + ":i4-time a model1:TemporalInstant ;"
            + "      rdfs:label \"Model1Class 4 Time\"^^xsd:string ;"
            + "      model1:dateTime \"2001-02-27T21:20:00.000Z\"^^xsd:dateTime ."
            + "      "
            + ":i5   a       model1:Model1Class ;"
            + "      rdfs:label \"Model1Class 5\"^^xsd:string ;"
            + "      model1:name \"Model1Class 5\"^^xsd:string ;"
            + "      model1:hasModel1ClassAssoc :i5-assoc ;"
            + "      model1:model1ClassId \"ID05\"^^xsd:string ."
            + ""
            + ":i5-assoc a model1:Model1ClassAssoc ;"
            + "      rdfs:label \"Model1Class 5 Assoc\"^^xsd:string ;"
            + "      model1:hasEntity :i5-event ."
            + "      "
            + ":i5-event a model1:Model1Event ;"
            + "      rdfs:label \"Model1Class 5 Event\"^^xsd:string ;"
            + "      model1:hasTemporalEntity :i5-time ."
            + ""
            + ":i5-time a model1:TemporalInstant ;"
            + "      rdfs:label \"Model1Class 5 Time\"^^xsd:string ;"
            + "      model1:dateTime \"2002-01-16T00:30:00.000Z\"^^xsd:dateTime ."
            + "      "
            + ":i6   a       model1:Model1Class ;"
            + "      rdfs:label \"Model1Class 6\"^^xsd:string ;"
            + "      model1:name \"Model1Class 6\"^^xsd:string ;"
            + "      model1:hasModel1ClassAssoc :i6-assoc ;"
            + "      model1:model1ClassId \"ID06\"^^xsd:string ."
            + ""
            + ":i6-assoc a model1:Model1ClassAssoc ;"
            + "      rdfs:label \"Model1Class 6 Assoc\"^^xsd:string ;"
            + "      model1:hasEntity :i6-event ."
            + "      "
            + ":i6-event a model1:Model1Event ;"
            + "      rdfs:label \"Model1Class 6 Event\"^^xsd:string ;"
            + "      model1:hasTemporalEntity :i6-time ."
            + ""
            + ":i6-time a model1:TemporalInstant ;"
            + "      rdfs:label \"Model1Class 6 Time\"^^xsd:string ;"
            + "      model1:dateTime \"2003-04-08T13:43:00.000Z\"^^xsd:dateTime .";
}
