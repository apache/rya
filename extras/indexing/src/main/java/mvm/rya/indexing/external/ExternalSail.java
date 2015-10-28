package mvm.rya.indexing.external;

/*
 * #%L
 * mvm.rya.indexing.accumulo
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import info.aduna.iteration.CloseableIteration;

import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailBase;
import org.openrdf.sail.helpers.SailConnectionWrapper;

public class ExternalSail extends SailBase {
    private final Sail s;
    private final ExternalProcessor processor;

    public ExternalSail(Sail s, ExternalProcessor processor) {
        this.s = s;
        this.processor = processor;
    }

    @Override
    protected SailConnection getConnectionInternal() throws SailException {
        return new ProcessingSailConnection();
    }

    @Override
    public boolean isWritable() throws SailException {
        return s.isWritable();
    }

    @Override
    public ValueFactory getValueFactory() {
        return s.getValueFactory();
    }

    @Override
    protected void shutDownInternal() throws SailException {
        s.shutDown();
    }

    private class ProcessingSailConnection extends SailConnectionWrapper {

        public ProcessingSailConnection() throws SailException {
            super(s.getConnection());
        }

        @Override
        public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(TupleExpr tupleExpr, Dataset dataset,
                BindingSet bindings, boolean includeInferred) throws SailException {
            if ((tupleExpr instanceof Projection) || (tupleExpr instanceof QueryRoot)) {
                TupleExpr processedExpression = processor.process(tupleExpr);
                return super.evaluate(processedExpression, dataset, bindings, includeInferred);
            } else {
                return super.evaluate(tupleExpr, dataset, bindings, includeInferred);
            }
            
        }
    }
}
