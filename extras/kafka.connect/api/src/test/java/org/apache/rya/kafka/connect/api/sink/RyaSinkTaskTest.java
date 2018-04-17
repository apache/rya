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
package org.apache.rya.kafka.connect.api.sink;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Unit tests the methods of {@link RyaSinkTask}.
 */
public class RyaSinkTaskTest {

    /**
     * A {@link RyaSinkTask} used to test against an in memory Sail instance.
     */
    private static final class InMemoryRyaSinkTask extends RyaSinkTask {

        private Sail sail = null;

        @Override
        protected void checkRyaInstanceExists(final Map<String, String> taskConfig) throws IllegalStateException {
            // Do nothing. Always assume the Rya Instance exists.
        }

        @Override
        protected Sail makeSail(final Map<String, String> taskConfig) {
            if(sail == null) {
                sail = new MemoryStore();
                sail.initialize();
            }
            return sail;
        }
    }

    @Test(expected = IllegalStateException.class)
    public void start_ryaInstanceDoesNotExist() {
        // Create the task that will be tested.
        final RyaSinkTask task = new RyaSinkTask() {
            @Override
            protected void checkRyaInstanceExists(final Map<String, String> taskConfig) throws IllegalStateException {
                throw new IllegalStateException("It doesn't exist.");
            }

            @Override
            protected Sail makeSail(final Map<String, String> taskConfig) { return null; }
        };

        // Since the rya instance does not exist, this will throw an exception.
        task.start(new HashMap<>());
    }

    @Test
    public void singleRecord() {
        // Create the Statements that will be put by the task.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Set<Statement> statements = Sets.newHashSet(
                vf.createStatement(
                        vf.createIRI("urn:Alice"),
                        vf.createIRI("urn:WorksAt"),
                        vf.createIRI("urn:Taco Shop"),
                        vf.createIRI("urn:graph1")),
                vf.createStatement(
                        vf.createIRI("urn:Bob"),
                        vf.createIRI("urn:TalksTo"),
                        vf.createIRI("urn:Charlie"),
                        vf.createIRI("urn:graph2")),
                vf.createStatement(
                        vf.createIRI("urn:Eve"),
                        vf.createIRI("urn:ListensTo"),
                        vf.createIRI("urn:Alice"),
                        vf.createIRI("urn:graph1")));

        // Create the task that will be tested.
        final InMemoryRyaSinkTask task = new InMemoryRyaSinkTask();

        // Setup the properties that will be used to configure the task. We don't actually need to set anything
        // here since we're always returning true for ryaInstanceExists(...) and use an in memory RDF store.
        final Map<String, String> props = new HashMap<>();

        try {
            // Start the task.
            task.start(props);

            // Put the statements as a SinkRecord.
            task.put( Collections.singleton(new SinkRecord("topic", 1, null, "key", null, statements, 0)) );

            // Flush the statements.
            task.flush(new HashMap<>());

            // Fetch the stored Statements to show they match the original set.
            final Set<Statement> fetched = new HashSet<>();

            final Sail sail = task.makeSail(props);
            try(SailConnection conn = sail.getConnection();
                    CloseableIteration<? extends Statement, SailException> it = conn.getStatements(null, null, null, false)) {
                while(it.hasNext()) {
                    fetched.add( it.next() );
                }
            }

            assertEquals(statements, fetched);

        } finally {
            // Stop the task.
            task.stop();
        }
    }

    @Test
    public void multipleRecords() {
        // Create the Statements that will be put by the task.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Set<Statement> batch1 = Sets.newHashSet(
                vf.createStatement(
                        vf.createIRI("urn:Alice"),
                        vf.createIRI("urn:WorksAt"),
                        vf.createIRI("urn:Taco Shop"),
                        vf.createIRI("urn:graph1")),
                vf.createStatement(
                        vf.createIRI("urn:Bob"),
                        vf.createIRI("urn:TalksTo"),
                        vf.createIRI("urn:Charlie"),
                        vf.createIRI("urn:graph2")));

        final Set<Statement> batch2 = Sets.newHashSet(
                vf.createStatement(
                        vf.createIRI("urn:Eve"),
                        vf.createIRI("urn:ListensTo"),
                        vf.createIRI("urn:Alice"),
                        vf.createIRI("urn:graph1")));

        // Create the task that will be tested.
        final InMemoryRyaSinkTask task = new InMemoryRyaSinkTask();

        // Setup the properties that will be used to configure the task. We don't actually need to set anything
        // here since we're always returning true for ryaInstanceExists(...) and use an in memory RDF store.
        final Map<String, String> props = new HashMap<>();

        try {
            // Start the task.
            task.start(props);

            // Put the statements as SinkRecords.
            final Collection<SinkRecord> records = Sets.newHashSet(
                    new SinkRecord("topic", 1, null, "key", null, batch1, 0),
                    new SinkRecord("topic", 1, null, "key", null, batch2, 1));
            task.put( records );

            // Flush the statements.
            task.flush(new HashMap<>());

            // Fetch the stored Statements to show they match the original set.
            final Set<Statement> fetched = new HashSet<>();

            final Sail sail = task.makeSail(props);
            try(SailConnection conn = sail.getConnection();
                    CloseableIteration<? extends Statement, SailException> it = conn.getStatements(null, null, null, false)) {
                while(it.hasNext()) {
                    fetched.add( it.next() );
                }
            }

            assertEquals(Sets.union(batch1, batch2), fetched);

        } finally {
            // Stop the task.
            task.stop();
        }
    }

    @Test
    public void flushBetweenPuts() {
        // Create the Statements that will be put by the task.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Set<Statement> batch1 = Sets.newHashSet(
                vf.createStatement(
                        vf.createIRI("urn:Alice"),
                        vf.createIRI("urn:WorksAt"),
                        vf.createIRI("urn:Taco Shop"),
                        vf.createIRI("urn:graph1")),
                vf.createStatement(
                        vf.createIRI("urn:Bob"),
                        vf.createIRI("urn:TalksTo"),
                        vf.createIRI("urn:Charlie"),
                        vf.createIRI("urn:graph2")));

        final Set<Statement> batch2 = Sets.newHashSet(
                vf.createStatement(
                        vf.createIRI("urn:Eve"),
                        vf.createIRI("urn:ListensTo"),
                        vf.createIRI("urn:Alice"),
                        vf.createIRI("urn:graph1")));

        // Create the task that will be tested.
        final InMemoryRyaSinkTask task = new InMemoryRyaSinkTask();

        // Setup the properties that will be used to configure the task. We don't actually need to set anything
        // here since we're always returning true for ryaInstanceExists(...) and use an in memory RDF store.
        final Map<String, String> props = new HashMap<>();

        try {
            // Start the task.
            task.start(props);

            // Put the statements with flushes between them.
            task.put( Collections.singleton(new SinkRecord("topic", 1, null, "key", null, batch1, 0)) );
            task.flush(new HashMap<>());
            task.put( Collections.singleton(new SinkRecord("topic", 1, null, "key", null, batch2, 1)) );
            task.flush(new HashMap<>());

            // Fetch the stored Statements to show they match the original set.
            final Set<Statement> fetched = new HashSet<>();

            final Sail sail = task.makeSail(props);
            try(SailConnection conn = sail.getConnection();
                    CloseableIteration<? extends Statement, SailException> it = conn.getStatements(null, null, null, false)) {
                while(it.hasNext()) {
                    fetched.add( it.next() );
                }
            }

            assertEquals(Sets.union(batch1, batch2), fetched);

        } finally {
            // Stop the task.
            task.stop();
        }
    }
}