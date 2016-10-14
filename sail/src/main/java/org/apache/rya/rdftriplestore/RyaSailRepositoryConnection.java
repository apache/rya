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

import org.openrdf.OpenRDFUtil;
import org.openrdf.model.Resource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.util.RDFLoader;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailConnection;

/**
 * The real reason for this is so that we can combine contexts from an input stream/reader and the given contexts in the add function
 */
public class RyaSailRepositoryConnection extends SailRepositoryConnection {

    protected RyaSailRepositoryConnection(SailRepository repository, SailConnection sailConnection) {
        super(repository, sailConnection);
    }

    @Override
    public void add(InputStream in, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException,
            RepositoryException {
        OpenRDFUtil.verifyContextNotNull(contexts);

        CombineContextsRdfInserter rdfInserter = new CombineContextsRdfInserter(this);
        rdfInserter.enforceContext(contexts);

        boolean localTransaction = startLocalTransaction();
        try {
            RDFLoader loader = new RDFLoader(getParserConfig(), getValueFactory());
            loader.load(in, baseURI, dataFormat, rdfInserter);

            conditionalCommit(localTransaction);
        } catch (RDFHandlerException e) {
            conditionalRollback(localTransaction);

            throw ((RepositoryException) e.getCause());
        } catch (RDFParseException e) {
            conditionalRollback(localTransaction);
            throw e;
        } catch (IOException e) {
            conditionalRollback(localTransaction);
            throw e;
        } catch (RuntimeException e) {
            conditionalRollback(localTransaction);
            throw e;
        }
    }

    @Override
    public void add(Reader reader, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException,
            RepositoryException {
        OpenRDFUtil.verifyContextNotNull(contexts);

        CombineContextsRdfInserter rdfInserter = new CombineContextsRdfInserter(this);
        rdfInserter.enforceContext(contexts);

        boolean localTransaction = startLocalTransaction();
        try {
            RDFLoader loader = new RDFLoader(getParserConfig(), getValueFactory());
            loader.load(reader, baseURI, dataFormat, rdfInserter);

            conditionalCommit(localTransaction);
        } catch (RDFHandlerException e) {
            conditionalRollback(localTransaction);

            throw ((RepositoryException) e.getCause());
        } catch (RDFParseException e) {
            conditionalRollback(localTransaction);
            throw e;
        } catch (IOException e) {
            conditionalRollback(localTransaction);
            throw e;
        } catch (RuntimeException e) {
            conditionalRollback(localTransaction);
            throw e;
        }
    }
}
