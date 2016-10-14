package org.apache.rya.indexing.accumulo.freetext;

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


import info.aduna.iteration.CloseableIteration;

import java.io.IOException;
import java.util.Set;

import org.apache.rya.indexing.FreeTextIndexer;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.IteratorFactory;
import org.apache.rya.indexing.SearchFunction;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryModelVisitor;

import com.google.common.base.Joiner;


//Indexing Node for freetext expressions to be inserted into execution plan 
//to delegate freetext portion of query to free text index
public class FreeTextTupleSet extends ExternalTupleSet {
    
    private Configuration conf;
    private FreeTextIndexer freeTextIndexer;
    private IndexingExpr filterInfo;
    

    public FreeTextTupleSet(IndexingExpr filterInfo, FreeTextIndexer freeTextIndexer) {
        this.filterInfo = filterInfo;
        this.freeTextIndexer = freeTextIndexer;
        this.conf = freeTextIndexer.getConf();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getBindingNames() {
        return filterInfo.getBindingNames();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note that we need a deep copy for everything that (during optimizations)
     * can be altered via {@link #visitChildren(QueryModelVisitor)}
     */
    public FreeTextTupleSet clone() {
        return new FreeTextTupleSet(filterInfo, freeTextIndexer);
    }

    @Override
    public double cardinality() {
        return 0.0; // No idea how the estimate cardinality here.
    }
    
    
    
    
    @Override
    public String getSignature() {
        
        return "(FreeTextTuple Projection) " + "variables: " + Joiner.on(", ").join(this.getBindingNames()).replaceAll("\\s+", " ");
    }
    
    
    
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof FreeTextTupleSet)) {
            return false;
        }

        FreeTextTupleSet arg = (FreeTextTupleSet) other;
        return this.filterInfo.equals(arg.filterInfo);
    }
    
    
    @Override
    public int hashCode() {
        int result = 17;
        result = 31*result + filterInfo.hashCode();
        
        return result;
    }
    
    

    /**
     * Returns an iterator over the result set of the contained {@link IndexExpr}.
     * <p>
     * Should be thread-safe (concurrent invocation {@link OfflineIterable} this
     * method can be expected with some query evaluators.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings)
            throws QueryEvaluationException {
        
      
        URI funcURI = filterInfo.getFunction();
        
        SearchFunction searchFunction = new SearchFunction() {

            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(String queryText,
                    StatementConstraints contraints) throws QueryEvaluationException {
                try {
                    CloseableIteration<Statement, QueryEvaluationException> statements = freeTextIndexer.queryText(
                            queryText, contraints);
                    return statements;
                } catch (IOException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public String toString() {
                return "TEXT";
            };
        };

        if (filterInfo.getArguments().length > 1) {
            throw new IllegalArgumentException("Index functions do not support more than two arguments.");
        }

        String queryText = filterInfo.getArguments()[0].stringValue();

        return IteratorFactory.getIterator(filterInfo.getSpConstraint(), bindings, queryText, searchFunction);
    }
    
}
