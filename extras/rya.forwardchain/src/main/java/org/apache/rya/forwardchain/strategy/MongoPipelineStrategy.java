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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQuery;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.forwardchain.ForwardChainException;
import org.apache.rya.forwardchain.rule.AbstractConstructRule;
import org.apache.rya.forwardchain.rule.Rule;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.apache.rya.mongodb.aggregation.AggregationPipelineQueryNode;
import org.apache.rya.mongodb.aggregation.SparqlToPipelineTransformVisitor;
import org.apache.rya.mongodb.batch.MongoDbBatchWriter;
import org.apache.rya.mongodb.batch.MongoDbBatchWriterConfig;
import org.apache.rya.mongodb.batch.MongoDbBatchWriterException;
import org.apache.rya.mongodb.batch.MongoDbBatchWriterUtils;
import org.apache.rya.mongodb.batch.collection.CollectionType;
import org.apache.rya.mongodb.batch.collection.MongoCollectionType;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.apache.rya.sail.config.RyaSailFactory;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.google.common.base.Preconditions;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * A rule execution strategy for MongoDB Rya that converts a single rule into an
 * aggregation pipeline whenever possible. Falls back on an internal
 * {@link SailExecutionStrategy} to handle any rules that can't be converted.
 */
public class MongoPipelineStrategy extends AbstractRuleExecutionStrategy {
    private static final Logger logger = Logger.getLogger(MongoPipelineStrategy.class);

    private static final int PIPELINE_BATCH_SIZE = 1000;

    private final SparqlToPipelineTransformVisitor pipelineVisitor;
    private final MongoCollection<Document> baseCollection;
    private final MongoDbBatchWriter<Document> batchWriter;
    private final MongoDBRyaDAO dao;
    private final SimpleMongoDBStorageStrategy storageStrategy = new SimpleMongoDBStorageStrategy();
    private final ConcurrentHashMap<Rule, Long> executionTimes = new ConcurrentHashMap<>();
    private final AbstractRuleExecutionStrategy backup;
    private final RyaQueryEngine<StatefulMongoDBRdfConfiguration> engine;
    private boolean usedBackup = false;

    /**
     * Initialize based on a configuration.
     * @param mongoConf Should contain database information; cannot be null. If
     *      passed a stateful configuration, uses the existing mongo client,
     *      otherwise creates one.
     */
    public MongoPipelineStrategy(final MongoDBRdfConfiguration mongoConf) throws ForwardChainException {
        Preconditions.checkNotNull(mongoConf);
        final String mongoDBName = mongoConf.getMongoDBName();
        final String collectionName = mongoConf.getTriplesCollectionName();
        mongoConf.setFlush(false);
        final StatefulMongoDBRdfConfiguration statefulConf;
        try {
            if (mongoConf instanceof StatefulMongoDBRdfConfiguration) {
                statefulConf = (StatefulMongoDBRdfConfiguration) mongoConf;
                this.dao = new MongoDBRyaDAO();
                this.dao.setConf(statefulConf);
                this.dao.init();
            }
            else {
                this.dao = RyaSailFactory.getMongoDAO(mongoConf);
                statefulConf = this.dao.getConf();
            }
        } catch (final RyaDAOException e) {
            throw new ForwardChainException("Can't connect to Rya.", e);
        }
        final MongoClient mongoClient = statefulConf.getMongoClient();
        final MongoDatabase mongoDB = mongoClient.getDatabase(mongoDBName);
        this.baseCollection = mongoDB.getCollection(collectionName);
        this.pipelineVisitor = new SparqlToPipelineTransformVisitor(this.baseCollection);
        this.engine = this.dao.getQueryEngine();
        this.backup = new SailExecutionStrategy(statefulConf);
        final MongoDbBatchWriterConfig writerConfig = MongoDbBatchWriterUtils.getMongoDbBatchWriterConfig(statefulConf);
        final CollectionType<Document> ct = new MongoCollectionType(baseCollection);
        this.batchWriter = new MongoDbBatchWriter<>(ct, writerConfig);
        try {
            this.batchWriter.start();
        } catch (final MongoDbBatchWriterException e) {
            throw new ForwardChainException("Error starting MongoDB batch writer", e);
        }
    }

    /**
     * Execute a CONSTRUCT rule by converting it into a pipeline, iterating
     * through the resulting documents, and inserting them back to the data
     * store as new triples. If pipeline conversion fails, falls back on
     * default execution strategy.
     * @param rule A construct query rule; not null.
     * @param metadata StatementMetadata to attach to new triples; not null.
     * @return The number of new triples inferred.
     * @throws ForwardChainException if execution fails.
     */
    @Override
    public long executeConstructRule(final AbstractConstructRule rule,
            final StatementMetadata metadata) throws ForwardChainException {
        Preconditions.checkNotNull(rule);
        logger.info("Applying inference rule " + rule + "...");
        final long timestamp = System.currentTimeMillis();
        // Get a pipeline that turns individual matches into triples
        List<Bson> pipeline = null;
        try {
            int requireSourceLevel = 0;
            if (!usedBackup) {
                // If we can assume derivation levels are set properly, we can optimize by
                // pruning any derived fact whose sources are all old information. (i.e. we can
                // infer that the pruned fact would have already been derived in a previous
                // step.) But if the backup strategy has ever been used, the source triples aren't
                // guaranteed to have derivation level set.
                requireSourceLevel = requiredLevel;
            }
            pipeline = toPipeline(rule, requireSourceLevel, timestamp);
        }
        catch (final ForwardChainException e) {
            logger.error(e);
        }
        if (pipeline == null) {
            if (backup == null) {
                logger.error("Couldn't convert " + rule + " to pipeline:");
                for (final String line : rule.getQuery().toString().split("\n")) {
                    logger.error("\t" + line);
                }
                throw new UnsupportedOperationException("Couldn't convert query to pipeline.");
            }
            else {
                logger.debug("Couldn't convert " + rule + " to pipeline:");
                for (final String line : rule.getQuery().toString().split("\n")) {
                    logger.debug("\t" + line);
                }
                logger.debug("Using fallback strategy.");
                usedBackup = true;
                return backup.executeConstructRule(rule, metadata);
            }
        }
        // Execute the pipeline
        for (final Bson step : pipeline) {
            logger.debug("\t" + step.toString());
        }
        final LongAdder count = new LongAdder();
        baseCollection.aggregate(pipeline)
            .allowDiskUse(true)
            .batchSize(PIPELINE_BATCH_SIZE)
            .forEach((final Document doc) -> {
                final DBObject dbo = BasicDBObject.parse(doc.toJson());
                final RyaStatement rstmt = storageStrategy.deserializeDBObject(dbo);
                if (!statementExists(rstmt)) {
                    count.increment();
                    doc.replace(SimpleMongoDBStorageStrategy.STATEMENT_METADATA, metadata.toString());
                    try {
                        batchWriter.addObjectToQueue(doc);
                    } catch (final MongoDbBatchWriterException e) {
                        logger.error("Couldn't insert " + rstmt, e);
                    }
                }
            });

        try {
            batchWriter.flush();
        } catch (final MongoDbBatchWriterException e) {
            throw new ForwardChainException("Error writing to Mongo", e);
        }
        logger.info("Added " + count + " new statements.");
        executionTimes.compute(rule, (r, previous) -> {
            if (previous != null && previous > timestamp) {
                return previous;
            }
            else {
                return timestamp;
            }
        });
        return count.longValue();
    }

    private boolean statementExists(final RyaStatement rstmt) {
        try {
            return engine.query(new RyaQuery(rstmt)).iterator().hasNext();
        } catch (final RyaDAOException e) {
            logger.error("Error querying for " + rstmt, e);
            return false;
        }
    }

    /**
     * Flush and close the batch writer, and shut down the backup
     * SailExecutionStrategy.
     * @throws ForwardChainException if the batch writer or backup strategy
     *  throw any errors.
     */
    @Override
    public void shutDown() throws ForwardChainException {
        backup.shutDown();
        try {
            batchWriter.shutdown();
        } catch (final MongoDbBatchWriterException e) {
            throw new ForwardChainException("Error shutting down batch writer", e);
        }
    }

    /**
     * Converts a construct rule into a series of documents representing
     * aggregation pipeline steps.
     * @param rule A construct query rule.
     * @param sourceLevel Only make derivations whose source triples have this
     *  derivation level or higher, i.e. took some number of forward chaining
     *  steps to infer. Set to zero to skip this check.
     * @param timestamp Timestamp to be set for all inferred triples.
     * @return An aggregation pipeline.
     * @throws ForwardChainException if pipeline construction fails.
     */
    private List<Bson> toPipeline(final AbstractConstructRule rule, final int sourceLevel,
            final long timestamp) throws ForwardChainException {
        TupleExpr tupleExpr = rule.getQuery().getTupleExpr();
        if (!(tupleExpr instanceof QueryRoot)) {
            tupleExpr = new QueryRoot(tupleExpr);
        }
        try {
            tupleExpr.visit(pipelineVisitor);
        } catch (final Exception e) {
            throw new ForwardChainException("Error converting construct rule to an aggregation pipeline", e);
        }
        if (tupleExpr instanceof QueryRoot) {
            final QueryRoot root = (QueryRoot) tupleExpr;
            if (root.getArg() instanceof AggregationPipelineQueryNode) {
                final AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) root.getArg();
                pipelineNode.distinct(); // require distinct triples
                pipelineNode.requireSourceDerivationDepth(sourceLevel);
                final long latestTime = executionTimes.getOrDefault(rule, 0L);
                if (latestTime > 0) {
                    pipelineNode.requireSourceTimestamp(latestTime);
                }
                return pipelineNode.getTriplePipeline(timestamp, false);
            }
        }
        return null;
    }
}
