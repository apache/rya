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
package org.apache.rya.mongodb.aggregation;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * MongoDB-specific query optimizer that replaces part or all of a SPARQL query
 * tree with a MongoDB aggregation pipeline.
 * <p>
 * Transforms query trees using {@link SparqlToPipelineTransformVisitor}. If
 * possible, this visitor will replace portions of the query tree, or the entire
 * query, with an equivalent aggregation pipeline (contained in an
 * {@link AggregationPipelineQueryNode}), thereby allowing query logic to be
 * evaluated by the MongoDB server rather than by the client.
 */
public class AggregationPipelineQueryOptimizer implements QueryOptimizer, Configurable {
    private StatefulMongoDBRdfConfiguration conf;
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        SparqlToPipelineTransformVisitor pipelineVisitor = new SparqlToPipelineTransformVisitor(conf);
        try {
            tupleExpr.visit(pipelineVisitor);
        } catch (Exception e) {
            logger.error("Error attempting to transform query using the aggregation pipeline", e);
        }
    }

    /**
     * @throws IllegalArgumentException if conf is not a {@link StatefulMongoDBRdfConfiguration}.
     */
    @Override
    public void setConf(Configuration conf) {
        Preconditions.checkNotNull(conf);
        Preconditions.checkArgument(conf instanceof StatefulMongoDBRdfConfiguration,
                "Expected an instance of %s; received %s",
                StatefulMongoDBRdfConfiguration.class.getName(), conf.getClass().getName());
        this.conf = (StatefulMongoDBRdfConfiguration) conf;
    }

    @Override
    public StatefulMongoDBRdfConfiguration getConf() {
        return conf;
    }
}
