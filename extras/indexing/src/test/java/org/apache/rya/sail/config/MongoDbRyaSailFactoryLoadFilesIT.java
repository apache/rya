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
package org.apache.rya.sail.config;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.helper.TestFile;
import org.apache.rya.helper.TestFileUtils;
import org.apache.rya.indexing.mongo.MongoPcjIntegrationTest.CountingResultHandler;
import org.apache.rya.mongodb.MongoRyaITBase;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.rdftriplestore.utils.RdfFormatUtils;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.After;
import org.junit.Test;

/**
 * Tests loading files through a MongoDB {@link RyaSailFactory}.
 */
public class MongoDbRyaSailFactoryLoadFilesIT extends MongoRyaITBase {
    private static final Logger log = Logger.getLogger(MongoDbRyaSailFactoryLoadFilesIT.class);

    private RyaSailRepository ryaRepository;

    @Override
    public void setupTest() throws Exception {
        super.setupTest();

        ryaRepository = createRyaSailRepository(conf);
    }

    @After
    public void tearDown() throws Exception {
        if (ryaRepository != null) {
            close(ryaRepository);
        }
    }

    private static RyaSailRepository createRyaSailRepository(final RdfCloudTripleStoreConfiguration config) throws SailException {
        log.info("Connecting to Sail Repository.");

        try {
            final Sail extSail = RyaSailFactory.getInstance(config);
            final RyaSailRepository repository = new RyaSailRepository(extSail);
            return repository;
        } catch (final Exception e) {
            throw new SailException("Failed to create Rya Sail Repository", e);
        }
    }

    /**
     * Shuts the repository down, releasing any resources that it keeps hold of.
     * Once shut down, the repository can no longer be used until it is
     * re-initialized.
     * @param repository the {@link SailRepository} to close.
     */
    private static void close(final SailRepository repository) {
        if (repository != null) {
            try {
                repository.shutDown();
            } catch (final RepositoryException e) {
                log.error("Encountered an error while closing Sail Repository", e);
            }
        }
    }

    /**
     * Closes the {@link SailRepositoryConnection}.
     * @param conn the {@link SailRepositoryConnection}.
     */
    private static void closeQuietly(final SailRepositoryConnection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (final RepositoryException e) {
                // quietly absorb this exception
            }
        }
    }

    private static void addTriples(final SailRepository repo, final InputStream triplesStream, final RDFFormat rdfFormat) throws RDFParseException, RepositoryException, IOException {
        SailRepositoryConnection conn = null;
        try {
            conn = repo.getConnection();
            conn.begin();
            conn.add(triplesStream, "", rdfFormat);
            conn.commit();
        } finally {
            closeQuietly(conn);
        }
    }

    private static int performTupleQuery(final String query, final RepositoryConnection conn) throws RepositoryException, MalformedQueryException, QueryEvaluationException, TupleQueryResultHandlerException {
        final TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.setMaxExecutionTime(10);
        final CountingResultHandler handler = new CountingResultHandler();
        tupleQuery.evaluate(handler);
        return handler.getCount();
    }

    @Test
    public void testFileLoading() throws Exception {
        log.info("Starting file loading test...");
        final String query = "SELECT * WHERE { ?s ?p ?o . }";
        final String deleteQuery = "DELETE WHERE { ?s ?p ?o . }";

        for (final TestFile testFile : TestFileUtils.TEST_FILES) {
            final String testFilePath = testFile.getPath();
            final RDFFormat rdfFormat = RdfFormatUtils.forFileName(testFilePath, null);
            log.info("Loading file \"" + testFilePath + "\" with RDFFormat: " + rdfFormat.getName());
            try (final InputStream rdfContent = getClass().getResourceAsStream(testFilePath)) {
                addTriples(ryaRepository, rdfContent, rdfFormat);
            }

            SailRepositoryConnection queryConn = null;
            try {
                log.info("Querying for triples in the repository from the " + rdfFormat.getName() + " file.");
                queryConn = ryaRepository.getConnection();
                final int count = performTupleQuery(query, queryConn);
                assertEquals("Expected number of triples not found in: " + testFilePath, testFile.getExpectedCount(), count);
            } finally {
                closeQuietly(queryConn);
            }

            SailRepositoryConnection deleteConn = null;
            try {
                log.info("Deleting triples in the repository from the " + rdfFormat.getName() + " file.");
                deleteConn = ryaRepository.getConnection();
                final Update update = deleteConn.prepareUpdate(QueryLanguage.SPARQL, deleteQuery);
                update.execute();
            } finally {
                closeQuietly(deleteConn);
            }
        }
        log.info("File loading test finished.");
    }
}