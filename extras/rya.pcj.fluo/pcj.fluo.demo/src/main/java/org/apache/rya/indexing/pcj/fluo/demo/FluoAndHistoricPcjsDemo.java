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
package org.apache.rya.indexing.pcj.fluo.demo;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.Snapshot;
import io.fluo.api.data.Bytes;
import io.fluo.api.mini.MiniFluo;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.rdftriplestore.RyaSailRepository;

/**
 * Demonstrates historicly added Rya statements that are stored within the core
 * Rya tables joining with newly streamed statements into the Fluo application.
 */
public class FluoAndHistoricPcjsDemo implements Demo {
    private static final Logger log = Logger.getLogger(FluoAndHistoricPcjsDemo.class);

    private static final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();

    // Employees
    private static final RyaURI alice = new RyaURI("http://Alice");
    private static final RyaURI bob = new RyaURI("http://Bob");
    private static final RyaURI charlie = new RyaURI("http://Charlie");
    private static final RyaURI frank = new RyaURI("http://Frank");

    // Patrons
    private static final RyaURI david = new RyaURI("http://David");
    private static final RyaURI eve = new RyaURI("http://Eve");
    private static final RyaURI george = new RyaURI("http://George");

    // Other People
    private static final RyaURI henry = new RyaURI("http://Henry");
    private static final RyaURI irene = new RyaURI("http://Irene");
    private static final RyaURI justin = new RyaURI("http://Justin");
    private static final RyaURI kristi = new RyaURI("http://Kristi");
    private static final RyaURI luke = new RyaURI("http://Luke");
    private static final RyaURI manny = new RyaURI("http://Manny");
    private static final RyaURI nate = new RyaURI("http://Nate");
    private static final RyaURI olivia = new RyaURI("http://Olivia");
    private static final RyaURI paul = new RyaURI("http://Paul");
    private static final RyaURI ross = new RyaURI("http://Ross");
    private static final RyaURI sally = new RyaURI("http://Sally");
    private static final RyaURI tim = new RyaURI("http://Tim");

    // Places
    private static final RyaURI coffeeShop = new RyaURI("http://CoffeeShop");
    private static final RyaURI burgerShop = new RyaURI("http://BurgerShop");
    private static final RyaURI cupcakeShop= new RyaURI("http://cupcakeShop");

    // Verbs
    private static final RyaURI talksTo = new RyaURI("http://talksTo");
    private static final RyaURI worksAt = new RyaURI("http://worksAt");

    /**
     * Used to pause the demo waiting for the presenter to hit the Enter key.
     */
    private final java.util.Scanner keyboard = new java.util.Scanner(System.in);

    @Override
    public void execute(
            final MiniAccumuloCluster accumulo,
            final Connector accumuloConn,
            final String ryaTablePrefix,
            final RyaSailRepository ryaRepo,
            final RepositoryConnection ryaConn,
            final MiniFluo fluo,
            final FluoClient fluoClient) throws DemoExecutionException {
        log.setLevel(Level.INFO);

        // 1. Introduce some RDF Statements that we are going to start with and
        //    pause so the presenter can introduce this information to the audience.
        final Set<RyaStatement> relevantHistoricStatements = Sets.newHashSet(
                new RyaStatement(eve, talksTo, charlie),
                new RyaStatement(david, talksTo, alice),
                new RyaStatement(alice, worksAt, coffeeShop),
                new RyaStatement(bob, worksAt, coffeeShop));

        log.info("We add some Statements that are relevant to the query we will compute:");
        prettyLogStatements(relevantHistoricStatements);
        waitForEnter();

        log.info("We also some more Satements that aren't realted to the query we will compute");
        final Set<RyaStatement> otherHistoricStatements = Sets.newHashSet(
                new RyaStatement(henry, worksAt, burgerShop),
                new RyaStatement(irene, worksAt, burgerShop),
                new RyaStatement(justin, worksAt, burgerShop),
                new RyaStatement(kristi, worksAt, burgerShop),
                new RyaStatement(luke, worksAt, burgerShop),
                new RyaStatement(manny, worksAt, cupcakeShop),
                new RyaStatement(nate, worksAt, cupcakeShop),
                new RyaStatement(olivia, worksAt, cupcakeShop),
                new RyaStatement(paul, worksAt, cupcakeShop),
                new RyaStatement(ross, worksAt, cupcakeShop),
                new RyaStatement(henry, talksTo, irene),
                new RyaStatement(henry, talksTo, justin),
                new RyaStatement(kristi, talksTo, irene),
                new RyaStatement(luke, talksTo, irene),
                new RyaStatement(sally, talksTo, paul),
                new RyaStatement(sally, talksTo, ross),
                new RyaStatement(sally, talksTo, kristi),
                new RyaStatement(tim, talksTo, nate),
                new RyaStatement(tim, talksTo, paul),
                new RyaStatement(tim, talksTo, kristi));

        log.info("Theese statements will also be inserted into the core Rya tables:");
        prettyLogStatements(otherHistoricStatements);
        waitForEnter();

        // 2. Load the statements into the core Rya tables.
        log.info("Loading the historic statements into Rya...");
        loadDataIntoRya(ryaConn, relevantHistoricStatements);
        loadDataIntoRya(ryaConn, otherHistoricStatements);
        log.info("");

        // 3. Introduce the query that we're going to load into Fluo and pause so that the
        //    presenter may show what they believe the expected output should be.
        final String sparql =
                "SELECT ?patron ?employee " +
                 "WHERE { " +
                     "?patron <http://talksTo> ?employee. " +
                     "?employee <http://worksAt> <http://CoffeeShop>. " +
                 "}";

        log.info("The following SPARQL query will be loaded into the Fluo application for incremental updates:");
        prettyLogSparql(sparql);
        waitForEnter();

        // 4. Write the query to Fluo and import the historic matches. Wait for the app to finish exporting results.
        log.info("Telling Fluo to maintain the query and import the historic Statement Pattern matches.");
        try {
            new CreatePcj().withRyaIntegration(fluoClient, ryaTablePrefix, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);
        } catch (MalformedQueryException | SailException | QueryEvaluationException | PcjException e) {
            throw new DemoExecutionException("Error while using Fluo to compute and export historic matches, so the demo can not continue. Exiting.", e);
        }

        log.info("Waiting for the fluo application to finish exporting the initial results...");
        fluo.waitForObservers();
        log.info("Historic result exporting finished.");
        log.info("");

        // 5. Show that the Fluo app exported the results to the PCJ table in Accumulo.
        final String pcjTableName = getPcjTableName(fluoClient, sparql);

        log.info("The following Binding Sets were exported to the '" + pcjTableName+ "' table in Accumulo:");
        Multimap<String, BindingSet> pcjResults = loadPcjResults(accumuloConn, pcjTableName);
        prettyLogPcjResults(pcjResults);
        waitForEnter();

        // 6. Introduce some new Statements that we will stream into the Fluo app.
        final RyaStatement newLeft = new RyaStatement(george, talksTo, frank);
        final RyaStatement newRight = new RyaStatement(frank, worksAt, coffeeShop);
        final RyaStatement joinLeft = new RyaStatement(eve, talksTo, bob);
        final RyaStatement joinRight = new RyaStatement(charlie, worksAt, coffeeShop);

        final Set<RyaStatement> relevantstreamedStatements = Sets.newHashSet(
                newLeft,
                newRight,
                joinLeft,
                joinRight);

        log.info("We stream these relevant Statements into Fluo and the core Rya tables:");
        log.info(prettyFormat(newLeft) + "          - Part of a new result");
        log.info(prettyFormat(newRight) + "      - Other part of a new result");
        log.info(prettyFormat(joinLeft) + "               - Joins with a historic <http://talksTo> statement");
        log.info(prettyFormat(joinRight) + "    - Joins with a historic <http://worksA>t statement");
        waitForEnter();

        final Set<RyaStatement> otherStreamedStatements = Sets.newHashSet(
                new RyaStatement(alice, talksTo, tim),
                new RyaStatement(bob, talksTo, tim),
                new RyaStatement(charlie, talksTo, tim),
                new RyaStatement(frank, talksTo, tim),
                new RyaStatement(david, talksTo, tim),
                new RyaStatement(eve, talksTo, sally),
                new RyaStatement(george, talksTo, sally),
                new RyaStatement(henry, talksTo, sally),
                new RyaStatement(irene, talksTo, sally),
                new RyaStatement(justin, talksTo, sally),
                new RyaStatement(kristi, talksTo, manny),
                new RyaStatement(luke, talksTo, manny),
                new RyaStatement(manny, talksTo, paul),
                new RyaStatement(nate, talksTo, manny),
                new RyaStatement(olivia, talksTo, manny),
                new RyaStatement(paul, talksTo, kristi),
                new RyaStatement(ross, talksTo, kristi),
                new RyaStatement(sally, talksTo, kristi),
                new RyaStatement(olivia, talksTo, kristi),
                new RyaStatement(olivia, talksTo, kristi));

        log.info("We also stream these irrelevant Statements into Fluo and the core Rya tables:");
        prettyLogStatements(otherStreamedStatements);
        waitForEnter();

        // 7. Insert the new triples into the core Rya tables and the Fluo app.
        loadDataIntoRya(ryaConn, relevantstreamedStatements);
        loadDataIntoFluo(fluoClient, relevantstreamedStatements);

        log.info("Waiting for the fluo application to finish exporting the newly streamed results...");
        fluo.waitForObservers();
        log.info("Streamed result exporting finished.");
        log.info("");

        // 8. Show the new results have been exported to the PCJ table in Accumulo.
        log.info("The following Binding Sets were expolrted to the '" + pcjTableName+ "' table in Accumulo:");
        pcjResults = loadPcjResults(accumuloConn, pcjTableName);
        prettyLogPcjResults(pcjResults);
        log.info("");
    }

    private void waitForEnter() {
        log.info("");
        log.info("Press [Enter] to continue the demo.");
        keyboard.nextLine();
    }

    private static void prettyLogSparql(final String sparql) {
        try {
            // Pretty print.
            final String[] lines = prettyFormatSparql(sparql);
            for(final String line : lines) {
                log.info(line);
            }
        } catch (final Exception e) {
            // Pretty print failed, so ugly print instead.
            log.info(sparql);
        }
    }

    private static void loadDataIntoFluo(final FluoClient fluoClient, final Set<RyaStatement> statements) {
        final InsertTriples insertTriples = new InsertTriples();
        for(final RyaStatement statement : statements) {
            insertTriples.insert(fluoClient, statement, Optional.<String>absent());
        }
    }

    private static String prettyFormat(final RyaStatement statement) {
        final RyaURI s = statement.getSubject();
        final RyaURI p = statement.getPredicate();
        final RyaType o = statement.getObject();
        return "<" + s.getData() + "> <"+ p.getData() + "> <" + o.getData() + ">";
    }

    private static void prettyLogStatements(final Set<RyaStatement> statements) {
        for(final RyaStatement statement : statements) {
            log.info("    " + prettyFormat(statement));
        }
    }

    private static String[] prettyFormatSparql(final String sparql) throws Exception {
        final SPARQLParser parser = new SPARQLParser();
        final SPARQLQueryRenderer renderer = new SPARQLQueryRenderer();
        final ParsedQuery pq = parser.parseQuery(sparql, null);
        final String prettySparql = renderer.render(pq);
        return StringUtils.split(prettySparql, '\n');
    }

    private static void loadDataIntoRya(final RepositoryConnection ryaConn, final Set<RyaStatement> statements) throws DemoExecutionException {
        for(final RyaStatement ryaStatement : statements) {
            final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
            try {
                ryaConn.add(statement);
            } catch (final RepositoryException e) {
                throw new DemoExecutionException("Could not load one of the historic statements into Rya, so the demo can not continue. Exiting.", e);
            }
        }
    }

    private static String getPcjTableName(final FluoClient fluoClient, final String sparql) {
        try(Snapshot snap = fluoClient.newSnapshot()) {
            final Bytes queryId = snap.get(Bytes.of(sparql), FluoQueryColumns.QUERY_ID);
            return snap.get(queryId, FluoQueryColumns.QUERY_RYA_EXPORT_TABLE_NAME).toString();
        }
    }

    /**
     * Scan accumulo for the results that are stored in a PCJ tablle. The
     * multimap stores a set of deserialized binding sets that were in the PCJ
     * table for every variable order that is found in the PCJ metadata.
     */
    private static Multimap<String, BindingSet> loadPcjResults(final Connector accumuloConn, final String pcjTableName) throws DemoExecutionException {
        final Multimap<String, BindingSet> fetchedResults = HashMultimap.create();

        try {
            // Get the variable orders the data was written to.
            final PcjTables pcjs = new PcjTables();
            final PcjMetadata pcjMetadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);

            // Scan Accumulo for the stored results.
            for(final VariableOrder varOrder : pcjMetadata.getVarOrders()) {
                final Scanner scanner = accumuloConn.createScanner(pcjTableName, new Authorizations());
                scanner.fetchColumnFamily( new Text(varOrder.toString()) );

                for(final Entry<Key, Value> entry : scanner) {
                    final byte[] serializedResult = entry.getKey().getRow().getBytes();
                    final BindingSet result = converter.convert(serializedResult, varOrder);
                    fetchedResults.put(varOrder.toString(), result);
                }
            }
        } catch(PcjException | TableNotFoundException | BindingSetConversionException e) {
            throw new DemoExecutionException("Couldn't fetch the binding sets that were exported to the PCJ table, so the demo can not continue. Exiting.", e);
        }

        return fetchedResults;
    }

    private static void prettyLogPcjResults(final Multimap<String, BindingSet> pcjResults) throws DemoExecutionException {
        final String varOrderString = pcjResults.keySet().iterator().next();
        final Collection<BindingSet> reuslts = pcjResults.get(varOrderString);
        for(final BindingSet result : reuslts) {
            log.info("    " + result);
        }
    }
}
