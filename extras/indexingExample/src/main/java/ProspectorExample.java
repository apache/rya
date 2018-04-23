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

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.utils.ConnectorFactory;
import org.apache.rya.api.persist.RdfEvalStatsDAO.CARDINALITY_OF;
import org.apache.rya.prospector.mr.Prospector;
import org.apache.rya.prospector.service.ProspectorServiceEvalStatsDAO;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;

import com.google.common.collect.Lists;

/**
 * Demonstrates how you can use the {@link Prospector} to count values that appear within an instance of Rya and
 * then use the {@link ProspectorServiceEvalStatsDAO} to fetch those counts.
 */
public class ProspectorExample {
    private static final Logger log = Logger.getLogger(RyaClientExample.class);

    private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

    private static final IRI ALICE = VALUE_FACTORY.createIRI("urn:alice");
    private static final IRI BOB = VALUE_FACTORY.createIRI("urn:bob");
    private static final IRI CHARLIE = VALUE_FACTORY.createIRI("urn:charlie");

    private static final IRI WORKS_AT = VALUE_FACTORY.createIRI("urn:worksAt");
    private static final IRI ADMIRES = VALUE_FACTORY.createIRI("urn:admires");
    private static final IRI LIVES_WITH = VALUE_FACTORY.createIRI("urn:livesWith");

    private static final IRI BURGER_JOINT = VALUE_FACTORY.createIRI("urn:burgerJoint");
    private static final IRI DONUT_SHOP= VALUE_FACTORY.createIRI("urn:donutShop");

    public static void main(final String[] args) throws Exception {
        setupLogging();

        // Configure Rya to use a mock instance.
        final AccumuloRdfConfiguration config = new AccumuloRdfConfiguration();
        config.useMockInstance(true);
        config.setTablePrefix("rya_");
        config.setUsername("user");
        config.setPassword("pass");
        config.setInstanceName("accumulo");

        // Load some data into Rya.
        final List<Statement> statements = Lists.newArrayList(
                VALUE_FACTORY.createStatement(ALICE, WORKS_AT, BURGER_JOINT),
                VALUE_FACTORY.createStatement(ALICE, ADMIRES, BOB),
                VALUE_FACTORY.createStatement(BOB, WORKS_AT, DONUT_SHOP),
                VALUE_FACTORY.createStatement(CHARLIE, WORKS_AT, DONUT_SHOP),
                VALUE_FACTORY.createStatement(CHARLIE, LIVES_WITH, BOB),
                VALUE_FACTORY.createStatement(BOB, LIVES_WITH, CHARLIE),
                VALUE_FACTORY.createStatement(BOB, LIVES_WITH, ALICE));

        final Sail sail = RyaSailFactory.getInstance(config);
        final SailConnection conn = sail.getConnection();
        log.info("Loading the following statements into a Mock instance of Accumulo Rya:");
        conn.begin();
        for(final Statement statement : statements) {
            log.info("    " + statement.toString());
            conn.addStatement(statement.getSubject(), statement.getPredicate(), statement.getObject());
        }
        conn.commit();
        conn.close();

        // Create the table that the Prospector's results will be written to.
        ConnectorFactory.connect(config)
                .tableOperations()
                .create("rya_prospects");

        // Run the Prospector using the configuration file that is in the resources directory.
        log.info("");
        log.info("Running the Map Reduce job that computes the Prospector results.");
        ToolRunner.run(new Prospector(), new String[]{ "src/main/resources/stats_cluster_config.xml" });

        // Print the table that was created by the Prospector.
        log.info("");
        log.info("The following cardinalities were written to the Prospector table:");
        final ProspectorServiceEvalStatsDAO dao = ProspectorServiceEvalStatsDAO.make(config);

        // Do each of the Subjects.
        double cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECT, Lists.newArrayList(ALICE));
        log.info("    subject: " + ALICE + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECT, Lists.newArrayList(BOB));
        log.info("    subject: " + BOB + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECT, Lists.newArrayList(CHARLIE));
        log.info("    subject: " + CHARLIE + ", cardinality: " + cardinality);

        // Do each of the Predicates.
        cardinality = dao.getCardinality(config, CARDINALITY_OF.PREDICATE, Lists.newArrayList(WORKS_AT));
        log.info("    predicate: " + WORKS_AT + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.PREDICATE, Lists.newArrayList(ADMIRES));
        log.info("    predicate: " + ADMIRES + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.PREDICATE, Lists.newArrayList(LIVES_WITH));
        log.info("    predicate: " + LIVES_WITH + ", cardinality: " + cardinality);

        // Do each of the Objects.
        cardinality = dao.getCardinality(config, CARDINALITY_OF.OBJECT, Lists.newArrayList(BURGER_JOINT));
        log.info("    object: " + BURGER_JOINT + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.OBJECT, Lists.newArrayList(DONUT_SHOP));
        log.info("    object: " + DONUT_SHOP + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.OBJECT, Lists.newArrayList(ALICE));
        log.info("    object: " + ALICE + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.OBJECT, Lists.newArrayList(BOB));
        log.info("    object: " + BOB + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.OBJECT, Lists.newArrayList(CHARLIE));
        log.info("    object: " + CHARLIE + ", cardinality: " + cardinality);

        // Do each of the Subject/Predicate pairs.
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTPREDICATE, Lists.newArrayList(ALICE, WORKS_AT));
        log.info("    subject/predicate: " + ALICE + "/" + WORKS_AT + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTPREDICATE, Lists.newArrayList(ALICE, ADMIRES));
        log.info("    subject/predicate: " + ALICE + "/" + ADMIRES + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTPREDICATE, Lists.newArrayList(BOB, WORKS_AT));
        log.info("    subject/predicate: " + BOB + "/" + WORKS_AT + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTPREDICATE, Lists.newArrayList(CHARLIE, WORKS_AT));
        log.info("    subject/predicate: " + CHARLIE + "/" + WORKS_AT + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTPREDICATE, Lists.newArrayList(CHARLIE, LIVES_WITH));
        log.info("    subject/predicate: " + CHARLIE + "/" + LIVES_WITH + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTPREDICATE, Lists.newArrayList(BOB, LIVES_WITH));
        log.info("    subject/predicate: " + BOB + "/" + LIVES_WITH + ", cardinality: " + cardinality);

        // Do each of the Subject/Object pairs.
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTOBJECT, Lists.newArrayList(ALICE, BURGER_JOINT));
        log.info("    subject/object: " + ALICE + "/" + BURGER_JOINT + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTOBJECT, Lists.newArrayList(ALICE, BOB));
        log.info("    subject/object: " + ALICE + "/" + BOB + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTOBJECT, Lists.newArrayList(BOB, DONUT_SHOP));
        log.info("    subject/object: " + ALICE + "/" + DONUT_SHOP + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTOBJECT, Lists.newArrayList(CHARLIE, DONUT_SHOP));
        log.info("    subject/object: " + CHARLIE + "/" + DONUT_SHOP + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTOBJECT, Lists.newArrayList(CHARLIE, BOB));
        log.info("    subject/object: " + CHARLIE + "/" + BOB + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTOBJECT, Lists.newArrayList(BOB, CHARLIE));
        log.info("    subject/object: " + BOB + "/" + CHARLIE + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.SUBJECTOBJECT, Lists.newArrayList(BOB, ALICE));
        log.info("    subject/object: " + BOB + "/" + ALICE + ", cardinality: " + cardinality);

        // Do each of the Predicate/Object pairs.
        cardinality = dao.getCardinality(config, CARDINALITY_OF.PREDICATEOBJECT, Lists.newArrayList(WORKS_AT, BURGER_JOINT));
        log.info("    predicate/object: " + WORKS_AT + "/" + BURGER_JOINT + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.PREDICATEOBJECT, Lists.newArrayList(ADMIRES, BOB));
        log.info("    predicate/object: " + ADMIRES + "/" + BOB + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.PREDICATEOBJECT, Lists.newArrayList(WORKS_AT, DONUT_SHOP));
        log.info("    predicate/object: " + WORKS_AT + "/" + DONUT_SHOP + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.PREDICATEOBJECT, Lists.newArrayList(LIVES_WITH, BOB));
        log.info("    predicate/object: " + LIVES_WITH + "/" + BOB + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.PREDICATEOBJECT, Lists.newArrayList(LIVES_WITH, CHARLIE));
        log.info("    predicate/object: " + LIVES_WITH + "/" + CHARLIE + ", cardinality: " + cardinality);
        cardinality = dao.getCardinality(config, CARDINALITY_OF.PREDICATEOBJECT, Lists.newArrayList(LIVES_WITH, ALICE));
        log.info("    predicate/object: " + LIVES_WITH + "/" + ALICE + ", cardinality: " + cardinality);
    }

    private static void setupLogging() {
        // Turn off all the loggers and customize how they write to the console.
        final Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.OFF);
        final ConsoleAppender ca = (ConsoleAppender) rootLogger.getAppender("stdout");
        ca.setLayout(new PatternLayout("%-5p - %m%n"));

        // Turn the logger used by the demo back on.
        log.setLevel(Level.INFO);
    }
}