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
package org.apache.rya.reasoning.mr;

import static org.eclipse.rdf4j.rio.RDFFormat.NO_CONTEXTS;
import static org.eclipse.rdf4j.rio.RDFFormat.NO_NAMESPACES;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.reasoning.Fact;
import org.apache.rya.reasoning.Schema;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.ntriples.NTriplesParser;
import org.eclipse.rdf4j.rio.rdfxml.RDFXMLParser;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

/**
 * Test the reasoner against Owl conformance tests in the database.
 */
public class ConformanceTest extends Configured implements Tool {
    static String TYPE = RDF.TYPE.stringValue();
    static String TEST = "http://www.w3.org/2007/OWL/testOntology#";
    static String TEST_CONSISTENCY = TEST + "ConsistencyTest";
    static String TEST_INCONSISTENCY = TEST + "InconsistencyTest";
    static String TEST_ENTAILMENT = TEST + "PositiveEntailmentTest";
    static String TEST_NONENTAILMENT = TEST + "NegativeEntailmentTest";
    static String TEST_ID = TEST + "identifier";
    static String TEST_DESC = TEST + "description";
    static String TEST_PROFILE = TEST + "profile";
    static String TEST_PREMISE = TEST + "rdfXmlPremiseOntology";
    static String TEST_CONCLUSION = TEST + "rdfXmlConclusionOntology";
    static String TEST_NONCONCLUSION = TEST + "rdfXmlNonConclusionOntology";
    static String TEST_RL = TEST + "RL";
    static String TEST_SEMANTICS = TEST + "semantics";
    static String TEST_RDFBASED = TEST + "RDF-BASED";

    private static class OwlTest extends AbstractRDFHandler {
        Value uri;
        String name;
        String description;
        String premise;
        String compareTo;
        Set<String> types = new HashSet<>();
        boolean success;
        Set<Statement> expected = new HashSet<>();
        Set<Statement> unexpected = new HashSet<>();
        Set<Statement> inferred = new HashSet<>();
        Set<Statement> error = new HashSet<>();
        @Override
        public void handleStatement(final Statement st) {
            if (types.contains(TEST_ENTAILMENT)) {
                expected.add(st);
            }
            else if (types.contains(TEST_NONENTAILMENT)) {
                unexpected.add(st);
            }
        }
        String type() {
            final StringBuilder sb = new StringBuilder();
            if (types.contains(TEST_CONSISTENCY)) {
                sb.append("{Consistency}");
            }
            if (types.contains(TEST_INCONSISTENCY)) {
                sb.append("{Inconsistency}");
            }
            if (types.contains(TEST_ENTAILMENT)) {
                sb.append("{Entailment}");
            }
            if (types.contains(TEST_NONENTAILMENT)) {
                sb.append("{Nonentailment}");
            }
            return sb.toString();
        }
    }

    private static class OutputCollector extends AbstractRDFHandler {
        Set<Statement> triples = new HashSet<>();
        @Override
        public void handleStatement(final Statement st) {
            triples.add(st);
        }
    }

    public static void main(final String[] args) throws Exception {
        ToolRunner.run(new ConformanceTest(), args);
    }

    @Override
    public int run(final String[] args) throws Exception {
        // Validate command
        if (args.length < 1 || args.length > 2) {
            System.out.println("Usage:\n");
            System.out.println("\tConformanceTest [configuration options] "
                + "<test-file> <temp-dir>\n");
            System.out.println("to load test data from an RDF file "
                + "(configuration property " + MRUtils.FORMAT_PROP
                + " specifies the format, default RDF/XML); or\n");
            System.out.println("\tConformanceTest [configuration options] <temp-dir>\n");
            System.out.println("to load test data from a Rya instance (specified "
                + "using standard configuration properties).\n");
            System.out.println("For each test given, run the reasoner over the "
                + "premise ontology using a temporary Mini Accumulo instance "
                + "at <temp-dir>, then report conformance results.");
            System.exit(1);
        }

        final Set<Value> conformanceTestURIs = new HashSet<>();
        Collection<OwlTest> conformanceTests = new LinkedList<>();
        final List<OwlTest> successes = new LinkedList<>();
        final List<OwlTest> failures = new LinkedList<>();
        final Configuration conf = getConf();
        Repository repo;
        File workingDir;

        // If tests are in a file, stick them in a repository for querying
        if (args.length == 2) {
            workingDir = new File(args[1]);
            RDFFormat inputFormat= RDFFormat.RDFXML;
            final String formatString = conf.get(MRUtils.FORMAT_PROP);
            if (formatString != null) {
                inputFormat = new RDFFormat(formatString,
                        Arrays.asList("application/n-triples", "text/plain"), Charset.forName("UTF-8"),
                        Collections.singletonList("nt"),
                        SimpleValueFactory.getInstance().createIRI("http://www.w3.org/ns/formats/"+formatString),
                        NO_NAMESPACES, NO_CONTEXTS);
            }
            repo = new SailRepository(new MemoryStore());
            repo.initialize();
            final RepositoryConnection conn = repo.getConnection();
            conn.add(new FileInputStream(args[0]), "", inputFormat);
            conn.close();
        }
        // Otherwise, get a Rya repository
        else {
            workingDir = new File(args[0]);
            repo = MRReasoningUtils.getRepository(conf);
            repo.initialize();
        }

        // Query for the tests we're interested in
        final RepositoryConnection conn = repo.getConnection();
        conformanceTestURIs.addAll(getTestURIs(conn, TEST_INCONSISTENCY));
        conformanceTestURIs.addAll(getTestURIs(conn, TEST_CONSISTENCY));
        conformanceTestURIs.addAll(getTestURIs(conn, TEST_ENTAILMENT));
        conformanceTestURIs.addAll(getTestURIs(conn, TEST_NONENTAILMENT));
        conformanceTests = getTests(conn, conformanceTestURIs);
        conn.close();
        repo.shutDown();

        // Set up a MiniAccumulo cluster and set up conf to connect to it
        final String username = "root";
        final String password = "root";
        final MiniAccumuloCluster mini = new MiniAccumuloCluster(workingDir, password);
        mini.start();
        conf.set(MRUtils.AC_INSTANCE_PROP, mini.getInstanceName());
        conf.set(MRUtils.AC_ZK_PROP, mini.getZooKeepers());
        conf.set(MRUtils.AC_USERNAME_PROP, username);
        conf.set(MRUtils.AC_PWD_PROP, password);
        conf.setBoolean(MRUtils.AC_MOCK_PROP, false);
        conf.set(MRUtils.TABLE_PREFIX_PROPERTY, "temp_");
        // Run the conformance tests
        int result;
        for (final OwlTest test : conformanceTests) {
            System.out.println(test.uri);
            result = runTest(conf, args, test);
            if (result != 0) {
                return result;
            }
            if (test.success) {
                successes.add(test);
                System.out.println("(SUCCESS)");
            }
            else {
                failures.add(test);
                System.out.println("(FAIL)");
            }
        }
        mini.stop();

        System.out.println("\n" + successes.size() + " successful tests:");
        for (final OwlTest test : successes) {
            System.out.println("\t[SUCCESS] " + test.type() + " " + test.name);
        }
        System.out.println("\n" + failures.size() + " failed tests:");
        for (final OwlTest test : failures) {
            System.out.println("\t[FAIL] " + test.type() + " " + test.name);
            System.out.println("\t\t(" + test.description + ")");
            for (final Statement triple : test.error) {
                if (test.types.contains(TEST_ENTAILMENT)) {
                    System.out.println("\t\tExpected: " + triple);
                }
                else if (test.types.contains(TEST_NONENTAILMENT)) {
                    System.out.println("\t\tUnexpected: " + triple);
                }
            }
        }
        return 0;
    }

    /**
     * Verify that we can infer the correct triples or detect an inconsistency.
     * @param   conf    Specifies working directory, etc.
     * @param   test   Contains premise/conclusion graphs, will store result
     * @return  Return value of the MapReduce job
     */
    int runTest(final Configuration conf, final String[] args, final OwlTest test)
            throws Exception {
        conf.setInt(MRReasoningUtils.STEP_PROP, 0);
        conf.setInt(MRReasoningUtils.SCHEMA_UPDATE_PROP, 0);
        conf.setBoolean(MRReasoningUtils.DEBUG_FLAG, true);
        conf.setBoolean(MRReasoningUtils.OUTPUT_FLAG, true);
        // Connect to MiniAccumulo and load the test
        final Repository repo = MRReasoningUtils.getRepository(conf);
        repo.initialize();
        final RepositoryConnection conn = repo.getConnection();
        conn.clear();
        conn.add(new StringReader(test.premise), "", RDFFormat.RDFXML);
        conn.close();
        repo.shutDown();
        // Run the reasoner
        final ReasoningDriver reasoner = new ReasoningDriver();
        final int result = ToolRunner.run(conf, reasoner, args);
        test.success = (result == 0);
        // Inconsistency test: successful if determined inconsistent
        if (test.types.contains(TEST_INCONSISTENCY)) {
            test.success = test.success && reasoner.hasInconsistencies();
        }
        // Consistency test: successful if determined consistent
        if (test.types.contains(TEST_CONSISTENCY)) {
            test.success = test.success && !reasoner.hasInconsistencies();
        }
        // Other types: we'll need to look at the inferred triples/schema
        if (test.types.contains(TEST_NONENTAILMENT)
            || test.types.contains(TEST_ENTAILMENT))  {
            System.out.println("Reading inferred triples...");
            // Read in the inferred triples from HDFS:
            final Schema schema = MRReasoningUtils.loadSchema(conf);
            final FileSystem fs = FileSystem.get(conf);
            final Path path = MRReasoningUtils.getOutputPath(conf, "final");
            final OutputCollector inferred = new OutputCollector();
            final NTriplesParser parser = new NTriplesParser();
            parser.setRDFHandler(inferred);
            if (fs.isDirectory(path)) {
                for (final FileStatus status : fs.listStatus(path)) {
                    final String s = status.getPath().getName();
                    if (s.startsWith(MRReasoningUtils.INCONSISTENT_OUT)
                        || s.startsWith(MRReasoningUtils.DEBUG_OUT)) {
                        continue;
                    }
                    final BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(status.getPath()), StandardCharsets.UTF_8));
                    parser.parse(br, "");
                    br.close();
                }
            }
            MRReasoningUtils.deleteIfExists(conf, "final");
            test.inferred.addAll(inferred.triples);
            // Entailment test: successful if expected triples were inferred
            if (test.types.contains(TEST_ENTAILMENT)) {
                // Check expected inferences against the inferred triples and
                // the schema reasoner
                for (final Statement st : test.expected) {
                    final Fact fact = new Fact(st);
                    if (!test.inferred.contains(st)
                            && !triviallyTrue(fact.getTriple(), schema)
                            && !schema.containsTriple(fact.getTriple())) {
                        test.error.add(st);
                    }
                }
            }
            // Non-entailment test: failure if non-expected triples inferred
            if (test.types.contains(TEST_NONENTAILMENT)) {
                for (final Statement st : test.unexpected) {
                    final Fact fact = new Fact(st);
                    if (test.inferred.contains(st)
                        || schema.containsTriple(fact.getTriple())) {
                        test.error.add(st);
                    }
                }
            }
            test.success = test.success && test.error.isEmpty();
        }
        conf.setBoolean(MRReasoningUtils.DEBUG_FLAG, false);
        MRReasoningUtils.clean(conf);
        return result;
    }

    /**
     * Query a connection for conformance tests matching a particular
     * test type.
     */
    Set<Value> getTestURIs(final RepositoryConnection conn, final String testType)
            throws IOException, RDF4JException {
        final Set<Value> testURIs = new HashSet<>();
        final TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL,
            "select ?test where { " +
            "?test <" + TYPE + "> <" + testType + "> .\n" +
            "?test <" + TEST_PROFILE + "> <" + TEST_RL + "> .\n" +
            "?test <" + TEST_SEMANTICS + "> <" + TEST_RDFBASED + "> .\n" +
            "}");
        final TupleQueryResult queryResult = query.evaluate();
        while (queryResult.hasNext()) {
            final BindingSet bindings = queryResult.next();
            testURIs.add(bindings.getValue("test"));
        }
        queryResult.close();
        return testURIs;
    }

    /**
     * Query a connection for conformance test details.
     */
    Collection<OwlTest> getTests(final RepositoryConnection conn, final Set<Value> testURIs)
            throws IOException, RDF4JException {
        final Map<Value, OwlTest> tests = new HashMap<>();
        final TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL,
            "select * where { " +
            "?test <" + TYPE + "> ?testType .\n" +
            "?test <" + TEST_PREMISE + "> ?graph .\n" +
            "?test <" + TEST_ID + "> ?name .\n" +
            "?test <" + TEST_DESC + "> ?description .\n" +
            "?test <" + TEST_PROFILE + "> <" + TEST_RL + "> .\n" +
            "?test <" + TEST_SEMANTICS + "> <" + TEST_RDFBASED + "> .\n" +
            "OPTIONAL {?test <" + TEST_CONCLUSION + "> ?conclusion .}\n" +
            "OPTIONAL {?test <" + TEST_NONCONCLUSION + "> ?nonentailed .}\n" +
            "}");
        final TupleQueryResult queryResult = query.evaluate();
        while (queryResult.hasNext()) {
            final BindingSet bindings = queryResult.next();
            final Value uri = bindings.getValue("test");
            if (testURIs.contains(uri)) {
                OwlTest test;
                if (tests.containsKey(uri)) {
                    test = tests.get(uri);
                }
                else {
                    test = new OwlTest();
                    test.uri = uri;
                    test.name = bindings.getValue("name").stringValue();
                    test.description = bindings.getValue("description").stringValue();
                    test.premise = bindings.getValue("graph").stringValue();
                    if (bindings.hasBinding("conclusion")) {
                        test.compareTo = bindings.getValue("conclusion").stringValue();
                    }
                    if (bindings.hasBinding("nonentailed")) {
                        test.compareTo = bindings.getValue("nonentailed").stringValue();
                    }
                    tests.put(uri, test);
                }
                test.types.add(bindings.getValue("testType").stringValue());
            }
        }
        for (final OwlTest test : tests.values()) {
            if (test.compareTo != null) {
                final RDFXMLParser parser = new RDFXMLParser();
                parser.setRDFHandler(test);
                parser.parse(new StringReader(test.compareTo), "");
            }
        }
        queryResult.close();
        return tests.values();
    }

    /**
     * Determine that a statement is trivially true for purposes of entailment
     * tests, such as an implicit "[bnode] type Ontology" triple or a
     * "[class] type Class" triple as long as the class exists.
     */
    boolean triviallyTrue(final Statement triple, final Schema schema) {
        final Resource s = triple.getSubject();
        final IRI p = triple.getPredicate();
        final Value o = triple.getObject();
        if (p.equals(RDF.TYPE)) {
            if (o.equals(OWL.ONTOLOGY)) {
                return true;
            }
            else if (o.equals(OWL.CLASS)) {
                return schema.hasClass(s);
            }
            else if ((o.equals(OWL.OBJECTPROPERTY)
                || o.equals(OWL.DATATYPEPROPERTY))
                && s instanceof IRI) {
                // Distinction not maintained, irrelevant to RL rules
                return schema.hasProperty((IRI) s);
            }
        }
        return false;
    }
}
