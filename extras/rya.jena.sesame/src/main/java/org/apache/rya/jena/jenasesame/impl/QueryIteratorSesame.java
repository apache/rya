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
package org.apache.rya.jena.jenasesame.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

/**
 * Sesame Query Iterator.
 */
public class QueryIteratorSesame extends QueryIteratorBase {
    private final TupleQueryResult result;

    /**
     * Creates a new instance of {@link QueryIteratorSesame}.
     * @param result the {@link TupleQueryResult}. (not {@code null})
     */
    public QueryIteratorSesame(final TupleQueryResult result) {
        this.result = checkNotNull(result);
    }

    @Override
    protected void closeIterator() {
        try {
            result.close();
        } catch (final QueryEvaluationException e) {
            throw new ARQException(e);
        }
    }

    @Override
    protected boolean hasNextBinding() {
        try {
            return result.hasNext();
        } catch (final QueryEvaluationException e) {
            throw new ARQException(e);
        }
    }

    @Override
    protected Binding moveToNextBinding() {
        try {
            final BindingSet bindingSet = result.next();
            final BindingMap arqBinding = BindingFactory.create();

            for (final String bindingName : result.getBindingNames()) {
                final Value value = bindingSet.getValue(bindingName);
                final Node node = Convert.valueToNode(value);
                arqBinding.add(Var.alloc(bindingName), node);
            }
            return arqBinding;
        } catch (final QueryEvaluationException e) {
            throw new ARQException(e);
        }
    }

    @Override
    public void output(final IndentedWriter out, final SerializationContext sCxt) {
    }

    @Override
    protected void requestCancel() {
        throw new NotImplemented();
    }
}