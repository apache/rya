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
/*
 * (c) Copyright 2009 Talis Information Ltd.
 * (c) Copyright 2010 Epimorphics Ltd.
 * All rights reserved.
 * [See end of file]
 */
package org.apache.rya.jena.jenasesame;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
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

/**
 * Tests the querying ability of {@link JenaSesame}.
 */
public class JenaSesameTest {
    private static final Logger log = Logger.getLogger(JenaSesameTest.class);

    @Test
    public void testQueryWithTurtleFile() throws Exception {
        Repository repo = null;
        RepositoryConnection queryConnection = null;
        QueryExecution queryExecution = null;
        try {
            repo = new SailRepository(new MemoryStore());
            repo.initialize();

            loadRdfFile(repo, "rdf_format_files/turtle_files/turtle_data.ttl");

            queryConnection = repo.getConnection();

            final Dataset dataset = JenaSesame.createDataset(queryConnection);

            final Model model = dataset.getDefaultModel();
            log.info(model.getNsPrefixMap());

            final String object = "susandillon@gmail.com";
            final String queryString = "prefix : <http://example/> SELECT * { ?s ?p '" + object + "' }";
            final Query query = QueryFactory.create(queryString);
            queryExecution = QueryExecutionFactory.create(query, dataset);

            final ResultSet results = queryExecution.execSelect();

            final String namespace = "http://rya.apache.org/jena/ns/contacts#";

            final List<String> expectedSubjects = Lists.newArrayList(namespace + "susan");
            final List<String> expectedPredicates = Lists.newArrayList(namespace + "emailAddress");

            int i = 0;
            while (results.hasNext()) {
                final QuerySolution binding = results.nextSolution();
                final Resource subject = (Resource) binding.get("s");
                log.info("Subject: " + subject.getURI());
                assertEquals(expectedSubjects.get(i), subject.getURI());
                final Resource predicate = (Resource) binding.get("p");
                log.info("Predicate: " + predicate.getURI());
                assertEquals(expectedPredicates.get(i), predicate.getURI());
                i++;
            }
        } catch (final Exception e) {
            log.error("Encountered an exception while performing query.", e);
        } finally {
            if (queryExecution != null) {
                queryExecution.close();
            }
            if (queryConnection != null) {
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
}

/*
 * (c) Copyright 2009 Talis Information Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */