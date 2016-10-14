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



import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 * Class ReorderJoinVisitor
 * Date: Apr 11, 2011
 * Time: 10:16:15 PM
 */
public class PushJoinDownVisitor extends QueryModelVisitorBase {
    @Override
    public void meet(Join node) throws Exception {
        super.meet(node);

        TupleExpr leftArg = node.getLeftArg();
        TupleExpr rightArg = node.getRightArg();

        /**
         * if join(join(1, 2), join(3,4))
         * should be:
         * join(join(join(1,2), 3), 4)
         */
        if (leftArg instanceof Join && rightArg instanceof Join) {
            Join leftJoin = (Join) leftArg;
            Join rightJoin = (Join) rightArg;
            TupleExpr right_LeftArg = rightJoin.getLeftArg();
            TupleExpr right_rightArg = rightJoin.getRightArg();
            Join inner = new Join(leftJoin, right_LeftArg);
            Join outer = new Join(inner, right_rightArg);
            node.replaceWith(outer);
        }

    }
}
