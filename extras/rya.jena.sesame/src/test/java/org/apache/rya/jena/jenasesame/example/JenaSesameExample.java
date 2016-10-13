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
package org.apache.rya.jena.jenasesame.example;

import java.io.File;
import java.net.URL;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.util.QueryExecUtils;
import org.apache.log4j.Logger;
import org.apache.rya.jena.jenasesame.JenaSesame;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.memory.MemoryStore;
import org.springframework.util.ResourceUtils;

/**
 * Example: Create an in-memory repository, load some RDF, and query it.
 */
public class JenaSesameExample {
    private static final Logger log = Logger.getLogger(JenaSesameExample.class);

    public static void main(final String args[]) throws Exception {
        Repository repo = null;
        RepositoryConnection addConnection = null;
        RepositoryConnection queryConnection = null;
        QueryExecution queryExecution = null;
        try {
            repo = new SailRepository(new MemoryStore());
            repo.initialize();

            // Load some data.
            addConnection = repo.getConnection();

            // Reads files relative from target/test-classes which should have been copied from src/test/resources
            final URL url = ClassLoader.getSystemResource("rdf_format_files/turtle_files/ex002.ttl");
            final File file = ResourceUtils.getFile(url);
            final String fileName = file.getAbsolutePath();
            final RDFFormat rdfFormat = RDFFormat.forFileName(fileName);

            log.info("Added RDF file with " + rdfFormat.getName() + " format: " + fileName);
            addConnection.add(file, "http://base/", rdfFormat);
            addConnection.close();

            queryConnection = repo.getConnection();

            final Dataset dataset = JenaSesame.createDataset(queryConnection);

            final Model model = dataset.getDefaultModel();
            log.info(model.getNsPrefixMap());

            final String object = "cindym@gmail.com";
            final String queryString = "prefix : <http://example/> SELECT * { ?s ?p '" + object + "' }";
            final Query query = QueryFactory.create(queryString);
            queryExecution = QueryExecutionFactory.create(query, dataset);
            QueryExecUtils.executeQuery(query, queryExecution);
        } catch (final Exception e) {
            log.error("Encountered an exception while performing query.", e);
        } finally {
            if (queryExecution != null) {
                queryExecution.close();
            }
            if (addConnection != null) {
                addConnection.close();
            }
            if (queryConnection != null) {
                queryConnection.close();
            }
            if (repo != null) {
                repo.shutDown();
            }
        }
    }
}