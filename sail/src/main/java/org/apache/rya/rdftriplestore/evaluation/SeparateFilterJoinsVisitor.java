package org.apache.rya.rdftriplestore.evaluation;

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



import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 * TODO: This might be a very bad thing. It may force all AND and not allow ORs?. Depends on how they do the bindings.
 * Class SeparateFilterJoinsVisitor
 * Date: Apr 11, 2011
 * Time: 10:16:15 PM
 */
public class SeparateFilterJoinsVisitor extends QueryModelVisitorBase {
    @Override
    public void meet(Filter node) throws Exception {
        super.meet(node);

        ValueExpr condition = node.getCondition();
        TupleExpr arg = node.getArg();
        if (!(arg instanceof Join)) {
            return;
        }

        Join join = (Join) arg;
        TupleExpr leftArg = join.getLeftArg();
        TupleExpr rightArg = join.getRightArg();

        if (leftArg instanceof StatementPattern && rightArg instanceof StatementPattern) {
            Filter left = new Filter(leftArg, condition);
            Filter right = new Filter(rightArg, condition);
            node.replaceWith(new Join(left, right));
        }

    }
}
