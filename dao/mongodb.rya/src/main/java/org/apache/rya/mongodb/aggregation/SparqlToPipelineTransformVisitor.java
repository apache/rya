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

import java.util.Arrays;

import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.bson.Document;
import org.eclipse.rdf4j.query.algebra.Distinct;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.MultiProjection;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.Reduced;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import com.google.common.base.Preconditions;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Visitor that transforms a SPARQL query tree by replacing as much of the tree
 * as possible with one or more {@code AggregationPipelineQueryNode}s.
 * <p>
 * Each {@link AggregationPipelineQueryNode} contains a MongoDB aggregation
 * pipeline which is equivalent to the replaced portion of the original query.
 * Evaluating this node executes the pipeline and converts the results into
 * query solutions. If only part of the query was transformed, the remaining
 * query logic (higher up in the query tree) can be applied to those
 * intermediate solutions as normal.
 * <p>
 * In general, processes the tree in bottom-up order: A leaf node
 * ({@link StatementPattern}) is replaced with a pipeline that matches the
 * corresponding statements. Then, if the parent node's semantics are supported
 * by the visitor, stages are appended to the pipeline and the subtree at the
 * parent node is replaced with the extended pipeline. This continues up the
 * tree until reaching a node that cannot be transformed, in which case that
 * node's child is now a single {@code AggregationPipelineQueryNode} (a leaf
 * node) instead of the previous subtree, or until the entire tree has been
 * subsumed into a single pipeline node.
 * <p>
 * Nodes which are transformed into pipeline stages:
 * <p><ul>
 * <li>A {@code StatementPattern} node forms the beginning of each pipeline.
 * <li>Single-argument operations {@link Projection}, {@link MultiProjection},
 * {@link Extension}, {@link Distinct}, and {@link Reduced} will be transformed
 * into pipeline stages whenever the child {@link TupleExpr} represents a
 * pipeline.
 * <li>A {@link Filter} operation will be appended to the pipeline when its
 * child {@code TupleExpr} represents a pipeline and the filter condition is a
 * type of {@link ValueExpr} understood by {@code AggregationPipelineQueryNode}.
 * <li>A {@link Join} operation will be appended to the pipeline when one child
 * is a {@code StatementPattern} and the other is an
 * {@code AggregationPipelineQueryNode}.
 * </ul>
 */
public class SparqlToPipelineTransformVisitor extends AbstractQueryModelVisitor<Exception> {
    private final MongoCollection<Document> inputCollection;

    /**
     * Instantiate a visitor directly from a {@link MongoCollection}.
     * @param inputCollection Stores triples.
     */
    public SparqlToPipelineTransformVisitor(MongoCollection<Document> inputCollection) {
        this.inputCollection = Preconditions.checkNotNull(inputCollection);
    }

    /**
     * Instantiate a visitor from a {@link MongoDBRdfConfiguration}.
     * @param conf Contains database connection information.
     */
    public SparqlToPipelineTransformVisitor(StatefulMongoDBRdfConfiguration conf) {
        Preconditions.checkNotNull(conf);
        MongoClient mongo = conf.getMongoClient();
        MongoDatabase db = mongo.getDatabase(conf.getRyaInstanceName());
        this.inputCollection = db.getCollection(conf.getTriplesCollectionName());
    }

    @Override
    public void meet(StatementPattern sp) {
        sp.replaceWith(new AggregationPipelineQueryNode(inputCollection, sp));
    }

    @Override
    public void meet(Join join) throws Exception {
        // If one branch is a single statement pattern, then try replacing the
        // other with a pipeline.
        AggregationPipelineQueryNode pipelineNode = null;
        StatementPattern joinWithSP = null;
        if (join.getRightArg() instanceof StatementPattern) {
            join.getLeftArg().visit(this);
            if (join.getLeftArg() instanceof AggregationPipelineQueryNode) {
                pipelineNode = (AggregationPipelineQueryNode) join.getLeftArg();
                joinWithSP = (StatementPattern) join.getRightArg();
            }
        }
        else if (join.getLeftArg() instanceof StatementPattern) {
            join.getRightArg().visit(this);
            if (join.getRightArg() instanceof AggregationPipelineQueryNode) {
                pipelineNode = (AggregationPipelineQueryNode) join.getRightArg();
                joinWithSP = (StatementPattern) join.getLeftArg();
            }
        }
        else {
            // Otherwise, visit the children to try to replace smaller subtrees
            join.visitChildren(this);
        }
        // If this is now a join between a pipeline node and a statement
        // pattern, add the join step at the end of the pipeline, and replace
        // this node with the extended pipeline node.
        if (pipelineNode != null && joinWithSP != null && pipelineNode.joinWith(joinWithSP)) {
            join.replaceWith(pipelineNode);
        }
    }

    @Override
    public void meet(Projection projectionNode) throws Exception {
        projectionNode.visitChildren(this);
        if (projectionNode.getArg() instanceof AggregationPipelineQueryNode && projectionNode.getParentNode() != null) {
            AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) projectionNode.getArg();
            if (pipelineNode.project(Arrays.asList(projectionNode.getProjectionElemList()))) {
                projectionNode.replaceWith(pipelineNode);
            }
        }
    }

    @Override
    public void meet(MultiProjection projectionNode) throws Exception {
        projectionNode.visitChildren(this);
        if (projectionNode.getArg() instanceof AggregationPipelineQueryNode && projectionNode.getParentNode() != null) {
            AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) projectionNode.getArg();
            if (pipelineNode.project(projectionNode.getProjections())) {
                projectionNode.replaceWith(pipelineNode);
            }
        }
    }

    @Override
    public void meet(Extension extensionNode) throws Exception {
        extensionNode.visitChildren(this);
        if (extensionNode.getArg() instanceof AggregationPipelineQueryNode && extensionNode.getParentNode() != null) {
            AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) extensionNode.getArg();
            if (pipelineNode.extend(extensionNode.getElements())) {
                extensionNode.replaceWith(pipelineNode);
            }
        }
    }

    @Override
    public void meet(Reduced reducedNode) throws Exception {
        reducedNode.visitChildren(this);
        if (reducedNode.getArg() instanceof AggregationPipelineQueryNode && reducedNode.getParentNode() != null) {
            reducedNode.replaceWith(reducedNode.getArg());
        }
    }

    @Override
    public void meet(Distinct distinctNode) throws Exception {
        distinctNode.visitChildren(this);
        if (distinctNode.getArg() instanceof AggregationPipelineQueryNode && distinctNode.getParentNode() != null) {
            AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) distinctNode.getArg();
            pipelineNode.distinct();
            distinctNode.replaceWith(pipelineNode);
        }
    }

    @Override
    public void meet(Filter filterNode) throws Exception {
        filterNode.visitChildren(this);
        if (filterNode.getArg() instanceof AggregationPipelineQueryNode && filterNode.getParentNode() != null) {
            AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) filterNode.getArg();
            if (pipelineNode.filter(filterNode.getCondition())) {
                filterNode.replaceWith(pipelineNode);
            }
        }
    }
}
