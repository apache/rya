package org.apache.rya.rdftriplestore.evaluation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rya.api.RdfTripleStoreConfiguration;
import org.apache.rya.api.domain.RangeURI;
import org.apache.rya.api.domain.RangeValue;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.BooleanLiteralImpl;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.QueryModelVisitorBase;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.RANGE;

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

/**
 * Class FilterTimeIndexVisitor
 * Date: Apr 11, 2011
 * Time: 10:16:15 PM
 */
public class FilterRangeVisitor extends QueryModelVisitorBase<Exception> {

    private final RdfTripleStoreConfiguration conf;
    private final Map<Var, RangeValue> rangeValues = new HashMap<Var, RangeValue>();

    public FilterRangeVisitor(final RdfTripleStoreConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public void meet(final Filter node) throws Exception {
        super.meet(node);

        final ValueExpr arg = node.getCondition();
        if (arg instanceof FunctionCall) {
            final FunctionCall fc = (FunctionCall) arg;
            if (RANGE.stringValue().equals(fc.getURI())) {
                //range(?var, start, end)
                final List<ValueExpr> valueExprs = fc.getArgs();
                if (valueExprs.size() != 3) {
                    throw new QueryEvaluationException("org.apache:range must have 3 parameters: variable, start, end");
                }
                final Var var = (Var) valueExprs.get(0);
                final ValueConstant startVc = (ValueConstant) valueExprs.get(1);
                final ValueConstant endVc = (ValueConstant) valueExprs.get(2);
                final Value start = startVc.getValue();
                final Value end = endVc.getValue();
                rangeValues.put(var, new RangeValue(start, end));
                node.setCondition(new ValueConstant(BooleanLiteralImpl.TRUE));
            }
        }
    }

    @Override
    public void meet(final StatementPattern node) throws Exception {
        super.meet(node);

        final Var subjectVar = node.getSubjectVar();
        final RangeValue subjRange = rangeValues.get(subjectVar);
        final Var predVar = node.getPredicateVar();
        final RangeValue predRange = rangeValues.get(predVar);
        final Var objVar = node.getObjectVar();
        final RangeValue objRange = rangeValues.get(objVar);
        if(subjRange != null) {
            subjectVar.setValue(new RangeURI(subjRange));//Assumes no blank nodes can be ranges
        }
        if(predRange != null) {
            predVar.setValue(new RangeURI(predRange));
        }
        if(objRange != null) {
            objVar.setValue(objRange);
        }
    }
}
