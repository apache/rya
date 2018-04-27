package org.apache.rya.rdftriplestore;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.apache.rya.rdftriplestore.utils.CombineContextsRdfInserter;
import org.eclipse.rdf4j.OpenRDFUtil;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.util.RDFLoader;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.sail.SailConnection;

/**
 * The real reason for this is so that we can combine contexts from an input stream/reader and the given contexts in the add function
 */
public class RyaSailRepositoryConnection extends SailRepositoryConnection {

    protected RyaSailRepositoryConnection(final SailRepository repository, final SailConnection sailConnection) {
        super(repository, sailConnection);
    }

    @Override
    public ParserConfig getParserConfig() {
        final ParserConfig parserConfig = super.getParserConfig();
        parserConfig.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
        return parserConfig;
    }

    @Override
    public void add(final InputStream in, final String baseURI, final RDFFormat dataFormat, final Resource... contexts) throws IOException, RDFParseException,
            RepositoryException {
        OpenRDFUtil.verifyContextNotNull(contexts);

        final CombineContextsRdfInserter rdfInserter = new CombineContextsRdfInserter(this);
        rdfInserter.enforceContext(contexts);

        final boolean localTransaction = startLocalTransaction();
        try {
            final RDFLoader loader = new RDFLoader(getParserConfig(), getValueFactory());
            loader.load(in, baseURI, dataFormat, rdfInserter);

            conditionalCommit(localTransaction);
        } catch (final RDFHandlerException e) {
            conditionalRollback(localTransaction);

            throw ((RepositoryException) e.getCause());
        } catch (final IOException | RuntimeException e) {
            conditionalRollback(localTransaction);
            throw e;
        }
    }

    @Override
    public void add(final Reader reader, final String baseURI, final RDFFormat dataFormat, final Resource... contexts) throws IOException, RDFParseException,
            RepositoryException {
        OpenRDFUtil.verifyContextNotNull(contexts);

        final CombineContextsRdfInserter rdfInserter = new CombineContextsRdfInserter(this);
        rdfInserter.enforceContext(contexts);

        final boolean localTransaction = startLocalTransaction();
        try {
            final RDFLoader loader = new RDFLoader(getParserConfig(), getValueFactory());
            loader.load(reader, baseURI, dataFormat, rdfInserter);

            conditionalCommit(localTransaction);
        } catch (final RDFHandlerException e) {
            conditionalRollback(localTransaction);

            throw ((RepositoryException) e.getCause());
        } catch (final IOException | RuntimeException e) {
            conditionalRollback(localTransaction);
            throw e;
        }
    }
}
