/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.accumulo.mr.merge;

import static org.apache.rya.accumulo.mr.merge.util.TestUtils.YESTERDAY;
import static org.apache.rya.accumulo.mr.merge.util.ToolConfigUtils.makeArgument;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Namespace;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import info.aduna.iteration.CloseableIteration;
import junit.framework.Assert;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.accumulo.mr.merge.common.InstanceType;
import org.apache.rya.accumulo.mr.merge.driver.AccumuloDualInstanceDriver;
import org.apache.rya.accumulo.mr.merge.util.AccumuloRyaUtils;
import org.apache.rya.accumulo.mr.merge.util.TestUtils;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.sail.config.RyaSailFactory;

public class RulesetCopyIT {
    private static final Logger log = Logger.getLogger(RulesetCopyIT.class);

    private static final boolean IS_MOCK = false;
    private static final String CHILD_SUFFIX = MergeTool.CHILD_SUFFIX;

    private static final String PARENT_PASSWORD = AccumuloDualInstanceDriver.PARENT_PASSWORD;
    private static final String PARENT_INSTANCE = AccumuloDualInstanceDriver.PARENT_INSTANCE;
    private static final String PARENT_TABLE_PREFIX = AccumuloDualInstanceDriver.PARENT_TABLE_PREFIX;
    private static final String PARENT_TOMCAT_URL = "http://rya-example-box:8080";

    private static final String CHILD_PASSWORD = AccumuloDualInstanceDriver.CHILD_PASSWORD;
    private static final String CHILD_INSTANCE = AccumuloDualInstanceDriver.CHILD_INSTANCE;
    private static final String CHILD_TABLE_PREFIX = AccumuloDualInstanceDriver.CHILD_TABLE_PREFIX;
    private static final String CHILD_TOMCAT_URL = "http://localhost:8080";

    private static final String QUERY_PREFIXES;

    private static AccumuloRdfConfiguration parentConfig;
    private static AccumuloRdfConfiguration childConfig;

    private static AccumuloDualInstanceDriver accumuloDualInstanceDriver;
    private static CopyTool rulesetTool = null;
    private static AccumuloRyaDAO parentDao;

    private static Map<String, String> prefixes = new HashMap<>();
    static {
        prefixes.put("test:", "http://example.com#");
        prefixes.put("alt:", "http://example.com/alternate#");
        prefixes.put("time:", "http://www.w3.org/2006/time#");
        prefixes.put("geo:", "http://www.opengis.net/ont/geosparql#");
        prefixes.put("fts:", "http://rdf.useekm.com/fts#");
        prefixes.put("tempo:", "tag:rya-rdf.org,2015:temporal#");
        prefixes.put("rdf:", RDF.NAMESPACE);
        prefixes.put("rdfs:", RDFS.NAMESPACE);
        prefixes.put("owl:", OWL.NAMESPACE);
        final StringBuilder sb = new StringBuilder();
        for (final String prefix : prefixes.keySet()) {
            sb.append("PREFIX " + prefix + " <" + prefixes.get(prefix) + ">\n");
        }
        QUERY_PREFIXES = sb.toString();
    }

    private static RyaURI substitute(final String uri) {
        for (final String prefix : prefixes.keySet()) {
            if (uri.startsWith(prefix)) {
                return new RyaURI(uri.replace(prefix, prefixes.get(prefix)));
            }
        }
        return new RyaURI(uri);
    }

    private static RyaStatement statement(final String s, final String p, final RyaType o) {
        final RyaStatement ryaStatement = new RyaStatement(substitute(s), substitute(p), o);
        ryaStatement.setTimestamp(YESTERDAY.getTime());
        return ryaStatement;
    }

    private static RyaStatement statement(final String s, final String p, final String o) {
        return statement(s, p, substitute(o));
    }

    private static RyaType literal(final String lit) {
        return new RyaType(lit);
    }

    private static RyaType literal(final String lit, final URI type) {
        return new RyaType(type, lit);
    }

    @BeforeClass
    public static void setUpPerClass() throws Exception {
        accumuloDualInstanceDriver = new AccumuloDualInstanceDriver(IS_MOCK, true, true, false, false);
        accumuloDualInstanceDriver.setUpInstances();
    }

    @Before
    public void setUpPerTest() throws Exception {
        parentConfig = accumuloDualInstanceDriver.getParentConfig();
        childConfig = accumuloDualInstanceDriver.getChildConfig();
        accumuloDualInstanceDriver.setUpTables();
        accumuloDualInstanceDriver.setUpConfigs();
        accumuloDualInstanceDriver.setUpDaos();
        parentDao = accumuloDualInstanceDriver.getParentDao();
    }

    @After
    public void tearDownPerTest() throws Exception {
        log.info("tearDownPerTest(): tearing down now.");
        accumuloDualInstanceDriver.tearDownDaos();
        accumuloDualInstanceDriver.tearDownTables();
    }

    @AfterClass
    public static void tearDownPerClass() throws Exception {
        if (rulesetTool != null) {
            rulesetTool.shutdown();
        }
        accumuloDualInstanceDriver.tearDown();
    }

    private AccumuloRyaDAO runRulesetCopyTest(final RyaStatement[] solutionStatements, final RyaStatement[] copyStatements,
            final RyaStatement[] irrelevantStatements, final String query, final int numSolutions, final boolean infer) throws Exception {
        log.info("Adding data to parent...");
        parentDao.add(Arrays.asList(solutionStatements).iterator());
        parentDao.add(Arrays.asList(copyStatements).iterator());
        parentDao.add(Arrays.asList(irrelevantStatements).iterator());

        log.info("Copying from parent tables:");
        for (final String table : accumuloDualInstanceDriver.getParentTableList()) {
            AccumuloRyaUtils.printTablePretty(table, parentConfig, false);
        }

        rulesetTool = new CopyTool();
        rulesetTool.setupAndRun(new String[] {
                makeArgument(MRUtils.AC_MOCK_PROP, Boolean.toString(IS_MOCK)),
                makeArgument(MRUtils.AC_INSTANCE_PROP, PARENT_INSTANCE),
                makeArgument(MRUtils.AC_USERNAME_PROP, accumuloDualInstanceDriver.getParentUser()),
                makeArgument(MRUtils.AC_PWD_PROP, PARENT_PASSWORD),
                makeArgument(MRUtils.TABLE_PREFIX_PROPERTY, PARENT_TABLE_PREFIX),
                makeArgument(MRUtils.AC_AUTH_PROP, accumuloDualInstanceDriver.getParentAuth()),
                makeArgument(MRUtils.AC_ZK_PROP, accumuloDualInstanceDriver.getParentZooKeepers()),
                makeArgument(CopyTool.PARENT_TOMCAT_URL_PROP, PARENT_TOMCAT_URL),
                makeArgument(MRUtils.AC_MOCK_PROP + CHILD_SUFFIX, Boolean.toString(IS_MOCK)),
                makeArgument(MRUtils.AC_INSTANCE_PROP + CHILD_SUFFIX, CHILD_INSTANCE),
                makeArgument(MRUtils.AC_USERNAME_PROP + CHILD_SUFFIX, accumuloDualInstanceDriver.getChildUser()),
                makeArgument(MRUtils.AC_PWD_PROP + CHILD_SUFFIX, CHILD_PASSWORD),
                makeArgument(MRUtils.TABLE_PREFIX_PROPERTY + CHILD_SUFFIX, CHILD_TABLE_PREFIX),
                makeArgument(MRUtils.AC_AUTH_PROP + CHILD_SUFFIX, accumuloDualInstanceDriver.getChildAuth() != null ? accumuloDualInstanceDriver.getChildAuth() : null),
                makeArgument(MRUtils.AC_ZK_PROP + CHILD_SUFFIX, accumuloDualInstanceDriver.getChildZooKeepers() != null ? accumuloDualInstanceDriver.getChildZooKeepers() : "localhost"),
                makeArgument(CopyTool.CHILD_TOMCAT_URL_PROP, CHILD_TOMCAT_URL),
                makeArgument(CopyTool.CREATE_CHILD_INSTANCE_TYPE_PROP, (IS_MOCK ? InstanceType.MOCK : InstanceType.MINI).toString()),
                makeArgument(CopyTool.QUERY_STRING_PROP, query),
                makeArgument(CopyTool.USE_COPY_QUERY_SPARQL, "true"),
                makeArgument(RdfCloudTripleStoreConfiguration.CONF_INFER, Boolean.toString(infer))
        });

        final Configuration toolConfig = rulesetTool.getConf();
        childConfig.set(MRUtils.AC_ZK_PROP, toolConfig.get(MRUtils.AC_ZK_PROP + CHILD_SUFFIX));
        MergeTool.setDuplicateKeys(childConfig);

        log.info("Finished running tool.");

        // Child instance has now been created
        final Connector childConnector = ConfigUtils.getConnector(childConfig);
        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setConnector(childConnector);
        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setUpTables();
        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setUpDao();
        final AccumuloRyaDAO childDao = accumuloDualInstanceDriver.getChildDao();

        log.info("Resulting child tables:");
        for (final String table : accumuloDualInstanceDriver.getChildTableList()) {
            AccumuloRyaUtils.printTablePretty(table, childConfig, false);
        }

        for (final RyaStatement solution : solutionStatements) {
            final Statement stmt = RyaToRdfConversions.convertStatement(solution);
            TestUtils.assertStatementInInstance("Child missing solution statement " + stmt,
                    1, solution, childDao, childConfig);
        }
        for (final RyaStatement copied : copyStatements) {
            final Statement stmt = RyaToRdfConversions.convertStatement(copied);
            TestUtils.assertStatementInInstance("Child missing relevant statement " + stmt,
                    1, copied, childDao, childConfig);
        }
        for (final RyaStatement irrelevant : irrelevantStatements) {
            final Statement stmt = RyaToRdfConversions.convertStatement(irrelevant);
            TestUtils.assertStatementInInstance("Should not have copied irrelevant statement " + stmt,
                    0, irrelevant, childDao, childConfig);
        }

        final Set<BindingSet> parentSolutions = runQuery(query, parentConfig);
        if (parentSolutions.isEmpty()) {
            log.info("No solutions to query in parent");
        }
        else {
            for (final BindingSet bs : parentSolutions) {
                log.info("Parent yields query solution: " + bs);
            }
        }
        final Set<BindingSet> childSolutions = runQuery(query, childConfig);
        if (childSolutions.isEmpty()) {
            log.info("No solutions to query in child");
        }
        else {
            for (final BindingSet bs : childSolutions) {
                log.info("Child yields query solution: " + bs);
            }
        }
        Assert.assertEquals("Query results should be the same:", parentSolutions, childSolutions);
        Assert.assertEquals("Incorrect number of solutions:", numSolutions, childSolutions.size());
        return childDao;
    }

    private Set<BindingSet> runQuery(final String query, final Configuration conf) throws Exception {
        SailRepository repository = null;
        SailRepositoryConnection conn = null;
        try {
            final Sail extSail = RyaSailFactory.getInstance(conf);
            repository = new SailRepository(extSail);
            repository.initialize();
            conn = repository.getConnection();
            final ResultHandler handler = new ResultHandler();
            final TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tq.evaluate(handler);
            return handler.getSolutions();
        }
        finally {
            if (conn != null) {
                conn.close();
            }
            if (repository != null) {
                repository.shutDown();
            }
        }
    }

    private static class ResultHandler implements TupleQueryResultHandler {
        private final Set<BindingSet> solutions = new HashSet<>();
        public Set<BindingSet> getSolutions() {
            return solutions;
        }
        @Override
        public void startQueryResult(final List<String> arg0) throws TupleQueryResultHandlerException {
        }
        @Override
        public void handleSolution(final BindingSet arg0) throws TupleQueryResultHandlerException {
            solutions.add(arg0);
        }
        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
        }
        @Override
        public void handleBoolean(final boolean arg0) throws QueryResultHandlerException {
        }
        @Override
        public void handleLinks(final List<String> arg0) throws QueryResultHandlerException {
        }
    }

    @Test
    public void testRulesetCopyTool() throws Exception {
        // Should be copied and are involved in the solution:
        final RyaStatement[] solutionStatements = {
            statement("test:FullProfessor1", "rdf:type", "test:FullProfessor"),
            statement("test:GraduateStudent1", "test:advisor", "test:FullProfessor1"),
            statement("test:FullProfessor1", "test:telephone", literal("123-456-7890")),
            statement("test:University1", "test:telephone", literal("555")),
            statement("test:FullProfessor1", "test:worksFor", "test:University1"),
            statement("test:FullProfessor1", "test:hired", literal("2001-01-01T04:01:02.000Z", XMLSchema.DATETIME)),
            statement("test:University1", "geo:asWKT", literal("Point(-77.03524 38.889468)", new URIImpl("http://www.opengis.net/ont/geosparql#wktLiteral")))
        };
        // These aren't solutions but should be copied:
        final RyaStatement[] copyStatements = {
            statement("test:FullProfessor2", "rdf:type", "test:FullProfessor"),
            statement("test:GraduateStudent1", "test:advisor", "test:AssistantProfessor1"),
            statement("test:GraduateStudent1", "test:telephone", literal("555-123-4567")),
            statement("test:FullProfessor1", "test:telephone", literal("567-8901")),
            statement("test:University1", "test:telephone", literal("800-123-4567"))
        };
        // Should not be copied:
        final RyaStatement[] irrelevantStatements = {
            statement("test:GraduateStudent2", "test:advisor", "test:FullProfessor1"),
            statement("test:UndergraduateStudent1", "rdf:type", "test:UndergraduateStudent"),
            statement("test:UndergraduateStudent2", "rdf:type", "test:UndergraduateStudent"),
            statement("test:GraduateStudent1", "rdf:type", "test:GraduateStudent"),
            statement("test:GraduateStudent1", "test:name", literal("GraduateStudent1")),
            statement("test:UndergraduateStudent2", "test:name", literal("UndergraduateStudent2")),
            statement("test:Course1", "test:name", literal("Course1")),
            statement("test:GraduateStudent1", "test:emailAddress", literal("GraduateStudent1@Department0.University0.edu")),
            statement("test:GraduateStudent1", "test:teachingAssistantOf", "test:Course1"),
            statement("test:GraduateStudent2", "test:undergraduateDegreeFrom", "test:University1"),
            statement("test:AssistantProfessor1", "test:teacherOf", "test:Course1"),
            statement("test:FullProfessor1", "test:telephone", literal("xxx-xxx-xxxx")),
            statement("test:University1", "test:telephone", literal("0000")),
            // If inferencing is disabled, these shouldn't matter:
            statement("test:employs", "owl:inverseOf", "test:worksFor"),
            statement("test:University1", "test:employs", "test:FullProfessor2")
        };

        final String query = QUERY_PREFIXES + "SELECT * {\n"
            + "    test:GraduateStudent1 test:advisor ?person .\n"
            + "    ?person rdf:type test:FullProfessor .\n"
            + "    ?person test:telephone ?number .\n"
            + "    ?person test:worksFor ?university .\n"
            + "    FILTER regex(?number, \"...-...-....\")\n"
            + "    ?university geo:asWKT ?place .\n"
            + "    FILTER regex(?number, \"^[0-9]\")\n"
            + "    ?university test:telephone ?universityNumber\n"
            + "    FILTER regex(?universityNumber, \"^5\")\n"
// Would be good to test index functions, but indexing is unreliable (see RYA-72):
            + "    ?person test:hired ?time .\n"
//            + "    FILTER(tempo:after(?time, '2000-01-01T01:01:03-08:00'))\n"
            + "}";

        final int parentNamespaceCount = 2;
        int childNamespaceCount = 0;
        parentDao.addNamespace("ns1", "http://www.example.com/ns1#");
        parentDao.addNamespace("ns2", "http://www.example.com/ns2#");
        // Run the test
        final AccumuloRyaDAO childDao = runRulesetCopyTest(solutionStatements, copyStatements, irrelevantStatements, query, 1, false);
        // Verify namespaces were copied
        final CloseableIteration<Namespace, RyaDAOException> nsIter = childDao.iterateNamespace();
        while (nsIter.hasNext()) {
            childNamespaceCount++;
            nsIter.next();
        }
        Assert.assertEquals("Incorrect number of namespaces copied to child:", parentNamespaceCount, childNamespaceCount);
        log.info("DONE");
    }

    /*
     * Tests subclass and subproperty inference. Based on standard LUBM query #5.
     */
    @Test
    public void testRulesetCopyHierarchy() throws Exception {
        final RyaStatement[] solutionStatements = {
                statement("test:p1", "rdf:type", "test:Professor"),
                statement("test:p1", "test:worksFor", "test:Department0"),
                statement("test:p2", "rdf:type", "test:FullProfessor"),
                statement("test:p2", "test:headOf", "test:Department0"),
        };
        final RyaStatement[] copyStatements = {
                // schema:
                statement("test:Professor", "rdfs:subClassOf", "test:Person"),
                statement("test:Student", "rdfs:subClassOf", "test:Person"),
                statement("test:GraduateStudent", "rdfs:subClassOf", "test:Student"),
                statement("test:FullProfessor", "rdfs:subClassOf", "test:Professor"),
                statement("test:AssistantProfessor", "rdfs:subClassOf", "test:Professor"),
                statement("test:worksFor", "rdfs:subPropertyOf", "test:memberOf"),
                statement("test:headOf", "rdfs:subPropertyOf", "test:worksFor"),
                // data:
                statement("test:s1", "rdf:type", "test:Student"),
                statement("test:ap1", "rdf:type", "test:AssistantProfessor"),
                statement("test:gs1", "rdf:type", "test:GraduateStudent"),
        };
        final RyaStatement[] otherStatements = {
                // schema:
                statement("test:worksFor", "rdfs:subPropertyOf", "test:affiliatedWith"),
                statement("test:Person", "rdfs:subClassOf", "test:Animal"),
                // data:
                statement("test:University0", "test:hasSubOrganizationOf", "test:Department0"),
                statement("test:a1", "rdf:type", "test:Animal")
        };
        final String query = QUERY_PREFIXES + "SELECT * {\n"
                + "    ?X rdf:type test:Person .\n"
                + "    ?X test:memberOf test:Department0 .\n"
                + "}";
        runRulesetCopyTest(solutionStatements, copyStatements, otherStatements, query, 2, true);
        log.info("DONE");
    }

    /**
     * Test the ruleset copy with owl:sameAs inference
     */
    @Test
    public void testRulesetCopySameAs() throws Exception {
        final String query = QUERY_PREFIXES + "SELECT * {\n"
                + "    {\n"
                + "        ?X test:worksFor test:Department0 .\n"
                + "        ?X rdf:type test:Student\n"
                + "    } UNION {\n"
                + "        test:p1 rdf:type test:Professor .\n"
                + "        test:p1 test:worksFor test:CSDept .\n"
                + "    }\n"
                + "}";
        final RyaStatement[] solutionStatements = {
            statement("test:s1", "test:worksFor", "test:CSDept"),
            statement("test:s1", "rdf:type", "test:Student"),
            statement("test:p1", "rdf:type", "test:Professor"),
            statement("alt:p1", "test:worksFor", "test:CSDept"),
            statement("test:CSDept", "owl:sameAs", "test:Department0"),
            statement("test:p1", "owl:sameAs", "alt:p1"),
            statement("test:JohnDoe", "owl:sameAs", "alt:p1")
        };
        final RyaStatement[] copyStatements = {
            statement("test:s2", "rdf:type", "test:Student"),
            statement("alt:s2", "test:worksFor", "test:CSDept"),
            statement("test:s3", "test:worksFor", "test:CSDept")
        };
        final RyaStatement[] otherStatements = {
            // sameAs inference only expands constants:
            statement("test:s2", "owl:sameAs", "alt:s2"),
            // sameAs inference not applied to rdf:type statements:
            statement("alt:p1", "rdf:type", "test:Professor"),
            statement("test:s3", "rdf:type", "alt:Student"),
            statement("test:Student", "owl:sameAs", "alt:Student")
        };
        runRulesetCopyTest(solutionStatements, copyStatements, otherStatements, query, 2, true);
        log.info("DONE");
    }

    /**
     * Test the ruleset copy with owl:TransitiveProperty inference.
     * Based on LUBM test query #11.
     */
    @Test
    public void testRulesetCopyTransitive() throws Exception {
        final String query = QUERY_PREFIXES + "SELECT * {\n"
                // Note: we get spurious results if the order of these are switched (see RYA-71):
                + "    ?X test:subOrganizationOf test:University0 .\n"
                + "    ?X rdf:type test:ResearchGroup .\n"
                + "}";
        final RyaStatement[] solutionStatements = {
                statement("test:subOrganizationOf", "rdf:type", "owl:TransitiveProperty"),
                statement("test:ResearchGroup0", "rdf:type", "test:ResearchGroup"),
                statement("test:ResearchGroup0", "test:subOrganizationOf", "test:Department0"),
                statement("test:Department0", "test:subOrganizationOf", "test:University0"),
                statement("test:Subgroup0", "rdf:type", "test:ResearchGroup"),
                statement("test:Subgroup0", "test:subOrganizationOf", "test:ResearchGroup0")
        };
        final RyaStatement[] copyStatements = {
                statement("test:ResearchGroupA", "rdf:type", "test:ResearchGroup"),
                statement("test:ResearchGroupA", "test:subOrganizationOf", "test:DepartmentA"),
                statement("test:DepartmentA", "test:subOrganizationOf", "test:UniversityA"),
                statement("test:OtherGroup0", "test:subOrganizationOf", "test:Department0"),
                statement("test:Department1", "test:subOrganizationOf", "test:University0")
        };
        final RyaStatement[] otherStatements = {
                statement("test:University0", "rdf:type", "test:University"),
                statement("test:Department0", "test:affiliatedWith", "test:University0")
        };
        runRulesetCopyTest(solutionStatements, copyStatements, otherStatements, query, 2, true);
        log.info("DONE");
    }

    /**
     * Test the ruleset copy with owl:inverseOf and owl:subPropertyOf inference.
     * Based on LUBM test query #13.
     */
    @Test
    public void testRulesetCopyInverse() throws Exception {
        final String query = QUERY_PREFIXES + "SELECT * {\n"
                + "    ?X rdf:type test:Person .\n"
                + "    test:University0 test:hasAlumnus ?X .\n"
                + "}";
        final RyaStatement[] solutionStatements = {
            statement("test:s1", "rdf:type", "test:Person"),
            statement("test:p1", "rdf:type", "test:Person"),
            statement("test:s1", "test:undergraduateDegreeFrom", "test:University0"),
            statement("test:p1", "test:doctoralDegreeFrom", "test:University0"),
            statement("test:undergraduateDegreeFrom", "rdfs:subPropertyOf", "test:degreeFrom"),
            statement("test:doctoralDegreeFrom", "rdfs:subPropertyOf", "test:degreeFrom"),
            statement("test:hasAlumnus", "owl:inverseOf", "test:degreeFrom")
        };
        final RyaStatement[] copyStatements = {
            statement("test:mastersDegreeFrom", "rdfs:subPropertyOf", "test:degreeFrom"),
            statement("test:s2", "test:mastersDegreeFrom", "test:University0"),
        };
        final RyaStatement[] otherStatements = {
            statement("test:p1", "test:mastersDegreeFrom", "test:University1"),
        };
        runRulesetCopyTest(solutionStatements, copyStatements, otherStatements, query, 2, true);
        log.info("DONE");
    }

    /**
     * Test the ruleset copy with owl:SymmetricProperty inference, combined with rdfs:subPropertyOf.
     */
    @Test
    public void testRulesetCopySymmetry() throws Exception {
        final String query = QUERY_PREFIXES + "SELECT * {\n"
                + "    ?X rdf:type test:Person .\n"
                + "    ?X test:knows test:Alice .\n"
                + "}";
        final RyaStatement[] solutionStatements = {
            statement("test:Alice", "test:knows", "test:Bob"),
            statement("test:Alice", "test:friendsWith", "test:Carol"),
            statement("test:Bob", "rdf:type", "test:Person"),
            statement("test:Carol", "rdf:type", "test:Person"),
            statement("test:friendsWith", "rdfs:subPropertyOf", "test:knows"),
            statement("test:knows", "rdf:type", "owl:SymmetricProperty")
        };
        final RyaStatement[] copyStatements = {
            statement("test:Alice", "rdf:type", "test:Person"),
            statement("test:Eve", "rdf:type", "test:Person")
        };
        final RyaStatement[] otherStatements = {
            statement("test:Carol", "test:knows", "test:Eve"),
            statement("test:Bob", "test:friendsWith", "test:Carol")
        };
        runRulesetCopyTest(solutionStatements, copyStatements, otherStatements, query, 2, true);
        log.info("DONE");
    }
}
