/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.mongodb.pcj;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.api.utils.IteratorWrapper;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.ParsedQueryUtil;
import org.apache.rya.indexing.pcj.matching.PCJOptimizerUtilities;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.mongo.MongoPcjDocuments;
import org.apache.rya.rdftriplestore.evaluation.ExternalBatchingIterator;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import info.aduna.iteration.CloseableIteration;

/**
 * Indexing Node for PCJs expressions to be inserted into execution plan to
 * delegate entity portion of query to {@link MongoPrecomputedJoinIndexer}.
 */
@DefaultAnnotation(NonNull.class)
public class PcjQueryNode extends ExternalTupleSet implements ExternalBatchingIterator {
    private static final Logger log = Logger.getLogger(PcjQueryNode.class);
    private final String tablename;
    private final PcjIndexer indexer;
    private final MongoPcjDocuments pcjDocs;

    /**
     * Creates a new {@link PcjQueryNode}.
     *
     * @param sparql - name of sparql query whose results will be stored in PCJ table
     * @param conf - Rya Configuration
     * @param tablename - name of an existing PCJ table
     *
     * @throws MalformedQueryException - The SPARQL query needs to contain a projection.
     */
    public PcjQueryNode(final String sparql, final String tablename, final MongoPcjDocuments pcjDocs) throws MalformedQueryException {
        this.pcjDocs = checkNotNull(pcjDocs);
        indexer = new MongoPrecomputedJoinIndexer();
        this.tablename = tablename;
        final SPARQLParser sp = new SPARQLParser();
        final ParsedTupleQuery pq = (ParsedTupleQuery) sp.parseQuery(sparql, null);
        final TupleExpr te = pq.getTupleExpr();
        Preconditions.checkArgument(PCJOptimizerUtilities.isPCJValid(te), "TupleExpr is an invalid PCJ.");

        final Optional<Projection> projection = new ParsedQueryUtil().findProjection(pq);
        if (!projection.isPresent()) {
            throw new MalformedQueryException("SPARQL query '" + sparql + "' does not contain a Projection.");
        }
        setProjectionExpr(projection.get());
    }

    /**
     * Creates a new {@link PcjQueryNode}.
     *
     * @param accCon - connection to a valid Accumulo instance
     * @param tablename - name of an existing PCJ table
     */
    public PcjQueryNode(final Configuration conf, final String tablename) {
        indexer = new MongoPrecomputedJoinIndexer();
        pcjDocs = indexer.getPcjStorage(conf);
        this.tablename = tablename;
    }

    /**
     * returns size of table for query planning
     */
    @Override
    public double cardinality() {
        double cardinality = 0;
        try {
            cardinality = pcjDocs.getPcjMetadata(tablename).getCardinality();
        } catch (final PcjException e) {
            log.error("The PCJ has not been created, so has no cardinality.", e);
        }
        return cardinality;
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final BindingSet bindingset)
            throws QueryEvaluationException {
        return this.evaluate(Collections.singleton(bindingset));
    }

    /**
     * Core evaluation method used during query evaluation - given a collection
     * of binding set constraints, this method finds common binding labels
     * between the constraints and table, uses those to build a query of mongodb,
     * and creates a solution binding set by iterating of
     * the scan results.
     *
     * @param bindingset - collection of {@link BindingSet}s to be joined with PCJ
     * @return - CloseableIteration over joined results
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final Collection<BindingSet> bindingset)
            throws QueryEvaluationException {

        if (bindingset.isEmpty()) {
            return new IteratorWrapper<BindingSet, QueryEvaluationException>(new HashSet<BindingSet>().iterator());
        }
        final CloseableIterator<BindingSet> iter = pcjDocs.getResults(tablename, new Authorizations(), bindingset);
        return new CloseableIteration<BindingSet, QueryEvaluationException>() {
            @Override
            public boolean hasNext() throws QueryEvaluationException {
                return iter.hasNext();
            }

            @Override
            public BindingSet next() throws QueryEvaluationException {
            	final BindingSet bs = iter.next();
                return bs;
            }

            @Override
            public void remove() throws QueryEvaluationException {
                iter.remove();
            }

            @Override
            public void close() throws QueryEvaluationException {
                try {
                    iter.close();
                } catch (final Exception e) {
                    throw new QueryEvaluationException(e.getMessage(), e);
                }
            }
        };
    }
    
    @Override
    public String getSignature() {
    	return "(Mongo PcjQueryNode) " + Joiner.on(", ").join(super.getTupleExpr().getProjectionElemList().getElements()).replaceAll("\\s+", " ");
    }
}