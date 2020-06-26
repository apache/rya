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
package org.apache.rya.forwardchain.strategy;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.api.persist.utils.RyaDAOHelper;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.forwardchain.ForwardChainException;
import org.apache.rya.forwardchain.rule.AbstractConstructRule;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailGraphQuery;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

/**
 * A naive but back-end-agnostic rule execution strategy that applies a
 * construct rule by submitting the associated query to a Rya SAIL, then
 * converting the resulting bindings (expecting variables "subject",
 * "predicate", and "object") into triples and inserting them into a Rya DAO.
 */
public class SailExecutionStrategy extends AbstractRuleExecutionStrategy {
    private static final Logger logger = Logger.getLogger(SailExecutionStrategy.class);

    private final RdfCloudTripleStoreConfiguration conf;

    private SailRepository repo = null;
    private SailRepositoryConnection conn = null;
    private RyaDAO<?> dao = null;
    private boolean initialized = false;

    /**
     * Initialize a SailExecutionStrategy with the given configuration.
     * @param conf Defines Rya connection and query parameters; not null.
     */
    public SailExecutionStrategy(RdfCloudTripleStoreConfiguration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    /**
     * Executes a CONSTRUCT query through the SAIL and inserts the results into
     * the DAO.
     * @param rule A construct query; not null.
     * @param metadata Metadata to add to any inferred triples; not null.
     * @return The number of inferred triples.
     * @throws ForwardChainException if query execution or data insert fails.
     */
    @Override
    public long executeConstructRule(AbstractConstructRule rule,
            StatementMetadata metadata) throws ForwardChainException {
        Preconditions.checkNotNull(rule);
        Preconditions.checkNotNull(metadata);
        if (!initialized) {
            initialize();
        }
        ParsedGraphQuery graphQuery = rule.getQuery();
        long statementsAdded = 0;
        logger.info("Applying inference rule " + rule + "...");
        for (String line : graphQuery.getTupleExpr().toString().split("\n")) {
            logger.debug("\t" + line);
        }
        InferredStatementHandler<?> handler = new InferredStatementHandler<>(dao, metadata);
        try {
            GraphQuery executableQuery = new SailGraphQuery(graphQuery, conn) { };
            executableQuery.evaluate(handler);
            statementsAdded = handler.getNumStatementsAdded();
            logger.info("Added " + statementsAdded + " inferred statements.");
            return statementsAdded;
        } catch (QueryEvaluationException e) {
            throw new ForwardChainException("Error evaluating query portion of construct rule", e);
        } catch (RDFHandlerException e) {
            throw new ForwardChainException("Error processing results of construct rule", e);
        }
    }

    /**
     * Connect to the Rya SAIL. If a DAO wasn't provided, instantiate one from
     * the configuration. 
     * @throws ForwardChainException if connecting fails.
     */
    @Override
    public void initialize() throws ForwardChainException {
        try {
            if (dao == null) {
                dao = getDAO();
            }
            repo = new SailRepository(RyaSailFactory.getInstance(conf));
            conn = repo.getConnection();
            initialized = true;
        } catch (Exception e) {
            shutDown();
            throw new ForwardChainException("Error connecting to SAIL", e);
        }
    }

    private RyaDAO<?> getDAO() throws RyaDAOException, ForwardChainException {
        if (ConfigUtils.getUseMongo(conf)) {
            MongoDBRdfConfiguration mongoConf;
            if (conf instanceof MongoDBRdfConfiguration) {
                mongoConf = (MongoDBRdfConfiguration) conf;
            }
            else {
                mongoConf = new MongoDBRdfConfiguration(conf);
            }
            return RyaSailFactory.getMongoDAO(mongoConf);
        }
        else {
            AccumuloRdfConfiguration accumuloConf;
            if (conf instanceof AccumuloRdfConfiguration) {
                accumuloConf = (AccumuloRdfConfiguration) conf;
            }
            else {
                accumuloConf = new AccumuloRdfConfiguration(conf);
            }
            try {
                return RyaSailFactory.getAccumuloDAO(accumuloConf);
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new ForwardChainException(e);
            }
        }
    }

    /**
     * Shut down the SAIL connection objects.
     */
    @Override
    public void shutDown() {
        initialized = false;
        if (conn != null) {
            try {
                conn.close();
            } catch (RepositoryException e) {
                logger.warn("Error closing SailRepositoryConnection", e);
            }
        }
        if (repo != null && repo.isInitialized()) {
            try {
                repo.shutDown();
            } catch (RepositoryException e) {
                logger.warn("Error shutting down SailRepository", e);
            }
        }
        try {
            if (dao != null && dao.isInitialized()) {
                dao.flush();
            }
        } catch (RyaDAOException e) {
            logger.warn("Error flushing DAO", e);
        }
    }

    private static class InferredStatementHandler<T extends RdfCloudTripleStoreConfiguration> extends AbstractRDFHandler {
        private RyaDAO<T> dao;
        private RyaQueryEngine<T> engine;
        private long numStatementsAdded = 0;
        private StatementMetadata metadata;

        InferredStatementHandler(RyaDAO<T> dao, StatementMetadata metadata) {
            this.dao = dao;
            this.engine = dao.getQueryEngine();
            this.metadata = metadata;
            this.engine.setConf(dao.getConf());
        }

        @Override
        public void handleStatement(Statement statement) {
            RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
            ryaStatement.setStatementMetadata(metadata);
            try {
                // Need to check whether the statement already exists, because
                // we need an accurate count of newly added statements.
                CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(engine, ryaStatement, engine.getConf());
                if (!iter.hasNext()) {
                    // Statement does not already exist
                    dao.add(ryaStatement);
                    numStatementsAdded++;
                }
            } catch (RyaDAOException e) {
                logger.error("Error handling inferred statement", e);
            }
        }

        public long getNumStatementsAdded() {
            return numStatementsAdded;
        }
    }
}
