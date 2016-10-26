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
package org.apache.rya.jena.jenasesame;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.memory.MemoryStore;
import org.springframework.util.ResourceUtils;

import com.google.common.collect.Lists;

/**
 * Tests the reasoning ability of {@link JenaSesame} using rule files.
 */
public class JenaReasoningWithRulesTest {
    private static final Logger log = Logger.getLogger(JenaReasoningWithRulesTest.class);

    @Test
    public void testReasonerWithNotation3File() throws Exception {
        final String namespace = "http://rya.apache.org/jena/ns/sports#";

        final List<Statement> expectedStatements = Lists.newArrayList(
            createStatement("Susan", "hasPlayer", "Bob", namespace)
        );

        testRdfFile("rdf_format_files/notation3_files/n3_data.n3", "rdf_format_files/notation3_files/rule_files/n3_rules.txt", expectedStatements);
    }

    @Test
    public void testReasonerWithOwlFile() throws Exception {
        final String namespace = "http://rya.apache.org/jena/ns/restaurant#";

        final List<Statement> expectedStatements = Lists.newArrayList(
            createStatement(namespace + "Bob", namespace + "waiterServesCustomer", namespace + "Alice"),
            createStatement(namespace + "Susan", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", namespace + "Waiter"),
            createStatement(namespace + "Susan", namespace + "servesTable", namespace + "Table2"),
            createStatement(namespace + "Susan", namespace + "waiterServesCustomer", namespace + "Ron"),
            createStatement(namespace + "Ron", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", namespace + "Customer"),
            createStatement(namespace + "Ron", namespace + "sitsAtTable", namespace + "Table2"),
            createStatement(namespace + "Ron", namespace + "hasOrdered", namespace + "True")
        );

        testRdfFile("rdf_format_files/rdfxml_files/rdfxml_data.owl", "rdf_format_files/rdfxml_files/rule_files/rdfxml_rules.rules", expectedStatements);
    }

    private static void testRdfFile(final String rdfRelativeFileName, final String rulesRelativeFileName, final List<Statement> expectedStatements) throws Exception {
        Repository repo = null;
        RepositoryConnection queryConnection = null;
        try {
            repo = new SailRepository(new MemoryStore());
            repo.initialize();

            // Load some data.
            loadRdfFile(repo, rdfRelativeFileName);

            queryConnection = repo.getConnection();

            final InfModel infModel = createInfModel(queryConnection, rulesRelativeFileName);

            final StmtIterator iterator = infModel.getDeductionsModel().listStatements();

            int i = 0;
            while (iterator.hasNext()) {
                final Statement statement = iterator.nextStatement();

                final Resource subject = statement.getSubject();
                final Property predicate = statement.getPredicate();
                final RDFNode object = statement.getObject();

                log.info(subject.toString() + " " + predicate.toString() + " " + object.toString());

                final Statement expectedStatement = expectedStatements.get(i);
                assertEquals(expectedStatement, statement);
                i++;
            }
        } catch (final Exception e) {
            log.error("Encountered an exception while running reasoner.", e);
            throw e;
        } finally {
            if (queryConnection != null && queryConnection.isOpen()) {
                queryConnection.close();
            }
            if (repo != null) {
                repo.shutDown();
            }
        }
    }

    private static void loadRdfFile(final Repository repo, final String rdfRelativeFileName) throws RepositoryException, RDFParseException, IOException {
        RepositoryConnection addConnection = null;
        try {
            // Load some data.
            addConnection = repo.getConnection();

            // Reads files relative from target/test-classes which should have been copied from src/test/resources
            final URL url = ClassLoader.getSystemResource(rdfRelativeFileName);
            final File file = ResourceUtils.getFile(url);
            final String fileName = file.getAbsolutePath();
            final RDFFormat rdfFormat = RDFFormat.forFileName(fileName);

            log.info("Added RDF file with " + rdfFormat.getName() + " format: " + fileName);
            addConnection.add(file, "http://base/", rdfFormat);
            addConnection.close();
        } finally {
            if (addConnection != null && addConnection.isOpen()) {
                addConnection.close();
            }
        }
    }

    private static InfModel createInfModel(final RepositoryConnection queryConnection, final String rulesRelativeFileName) throws FileNotFoundException {
        final Dataset dataset = JenaSesame.createDataset(queryConnection);

        final Model model = dataset.getDefaultModel();
        log.info(model.getNsPrefixMap());

        final URL rulesUrl = ClassLoader.getSystemResource(rulesRelativeFileName);
        final File rulesFile = ResourceUtils.getFile(rulesUrl);
        final String rulesFileName = rulesFile.getAbsolutePath();

        final Reasoner reasoner = new GenericRuleReasoner(Rule.rulesFromURL(rulesFileName));

        final InfModel infModel = ModelFactory.createInfModel(reasoner, model);
        return infModel;
    }

    private static Statement createStatement(final String subject, final String predicate, final String object, final String namespace) {
        return createStatement(namespace + subject, namespace + predicate, namespace + object);
    }

    private static Statement createStatement(final String subject, final String predicate, final String object) {
        return new StatementImpl(new ResourceImpl(subject), new PropertyImpl(predicate), new ResourceImpl(object));
    }
}