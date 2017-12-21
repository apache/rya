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

import java.util.Map;

import org.bson.Document;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.base.Preconditions;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;

import info.aduna.iteration.CloseableIteration;

/**
 * An iterator that converts the documents resulting from an
 * {@link AggregationPipelineQueryNode} into {@link BindingSet}s.
 */
public class PipelineResultIteration implements CloseableIteration<BindingSet, QueryEvaluationException> {
    private static final int BATCH_SIZE = 1000;
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    private final MongoCursor<Document> cursor;
    private final Map<String, String> varToOriginalName;
    private final BindingSet bindings;
    private BindingSet nextSolution = null;

    /**
     * Constructor.
     * @param aggIter Iterator of documents in AggregationPipelineQueryNode's
     *  intermediate solution representation.
     * @param varToOriginalName A mapping from field names in the pipeline
     *  result documents to equivalent variable names in the original query.
     *  Where an entry does not exist for a field, the field name and variable
     *  name are assumed to be the same.
     * @param bindings A partial solution. May be empty.
     */
    public PipelineResultIteration(AggregateIterable<Document> aggIter,
            Map<String, String> varToOriginalName,
            BindingSet bindings) {
        this.varToOriginalName = Preconditions.checkNotNull(varToOriginalName);
        this.bindings = Preconditions.checkNotNull(bindings);
        Preconditions.checkNotNull(aggIter);
        aggIter.batchSize(BATCH_SIZE);
        this.cursor = aggIter.iterator();
    }

    private void lookahead() {
        while (nextSolution == null && cursor.hasNext()) {
            nextSolution = docToBindingSet(cursor.next());
        }
    }

    @Override
    public boolean hasNext() throws QueryEvaluationException {
        lookahead();
        return nextSolution != null;
    }

    @Override
    public BindingSet next() throws QueryEvaluationException {
        lookahead();
        BindingSet solution = nextSolution;
        nextSolution = null;
        return solution;
    }

    /**
     * @throws UnsupportedOperationException always.
     */
    @Override
    public void remove() throws QueryEvaluationException {
        throw new UnsupportedOperationException("remove() undefined for query result iteration");
    }

    @Override
    public void close() throws QueryEvaluationException {
        cursor.close();
    }

    private QueryBindingSet docToBindingSet(Document result) {
        QueryBindingSet bindingSet = new QueryBindingSet(bindings);
        Document valueSet = result.get(AggregationPipelineQueryNode.VALUES, Document.class);
        Document typeSet = result.get(AggregationPipelineQueryNode.TYPES, Document.class);
        if (valueSet != null) {
            for (Map.Entry<String, Object> entry : valueSet.entrySet()) {
                String fieldName = entry.getKey();
                String valueString = entry.getValue().toString();
                String typeString = typeSet == null ? null : typeSet.getString(fieldName);
                String varName = varToOriginalName.getOrDefault(fieldName, fieldName);
                Value varValue;
                if (typeString == null || typeString.equals(XMLSchema.ANYURI.stringValue())) {
                    varValue = VF.createURI(valueString);
                }
                else {
                    varValue = VF.createLiteral(valueString, VF.createURI(typeString));
                }
                Binding existingBinding = bindingSet.getBinding(varName);
                // If this variable is not already bound, add it.
                if (existingBinding == null) {
                    bindingSet.addBinding(varName, varValue);
                }
                // If it's bound to something else, the solutions are incompatible.
                else if (!existingBinding.getValue().equals(varValue)) {
                    return null;
                }
            }
        }
        return bindingSet;
    }
}
