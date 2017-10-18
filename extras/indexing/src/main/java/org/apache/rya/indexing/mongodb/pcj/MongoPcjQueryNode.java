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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.api.utils.IteratorWrapper;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.ParsedQueryUtil;
import org.apache.rya.indexing.pcj.matching.PCJOptimizerUtilities;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.mongo.MongoPcjDocuments;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.apache.rya.rdftriplestore.evaluation.ExternalBatchingIterator;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Indexing Node for PCJs expressions to be inserted into execution plans.
 */
@DefaultAnnotation(NonNull.class)
public class MongoPcjQueryNode extends ExternalTupleSet implements ExternalBatchingIterator {
    private static final Logger log = Logger.getLogger(MongoPcjQueryNode.class);
    private final String pcjId;
    private final MongoPcjDocuments pcjDocs;

    /**
     * Creates a new {@link MongoPcjQueryNode}.
     *
     * @param sparql - sparql query whose results will be stored in PCJ document. (not empty of null)
     * @param pcjId - name of an existing PCJ. (not empty or null)
     * @param pcjDocs - {@link MongoPcjDocuments} used to maintain PCJs in mongo. (not null)
     *
     * @throws MalformedQueryException - The SPARQL query needs to contain a projection.
     */
    public MongoPcjQueryNode(final String sparql, final String pcjId, final MongoPcjDocuments pcjDocs) throws MalformedQueryException {
        checkArgument(!Strings.isNullOrEmpty(sparql));
        checkArgument(!Strings.isNullOrEmpty(pcjId));
        this.pcjDocs = checkNotNull(pcjDocs);
        this.pcjId = pcjId;
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
     * Creates a new {@link MongoPcjQueryNode}.
     *
     * @param conf - configuration to use to connect to mongo. (not null)
     * @param pcjId - name of an existing PCJ. (not empty or null)
     */
    public MongoPcjQueryNode(final Configuration conf, final String pcjId) {
        checkNotNull(conf);
        checkArgument(conf instanceof StatefulMongoDBRdfConfiguration,
                "The configuration must be a StatefulMongoDBRdfConfiguration, found: " + conf.getClass().getSimpleName());
        checkArgument(!Strings.isNullOrEmpty(pcjId));
        final StatefulMongoDBRdfConfiguration statefulConf = (StatefulMongoDBRdfConfiguration) conf;
        pcjDocs = new MongoPcjDocuments(statefulConf.getMongoClient(), statefulConf.getRyaInstanceName());
        this.pcjId = checkNotNull(pcjId);
    }

    /**
     * returns size of table for query planning
     */
    @Override
    public double cardinality() {
        double cardinality = 0;
        try {
            cardinality = pcjDocs.getPcjMetadata(pcjId).getCardinality();
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

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final Collection<BindingSet> bindingset)
            throws QueryEvaluationException {

        if (bindingset.isEmpty()) {
            return new IteratorWrapper<BindingSet, QueryEvaluationException>(new HashSet<BindingSet>().iterator());
        }
        final CloseableIterator<BindingSet> iter = pcjDocs.getResults(pcjId, bindingset);
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