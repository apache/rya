package org.apache.rya.indexing.IndexPlanValidator;

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


import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.collect.Sets;




public class IndexPlanValidator implements TupleValidator {

    private boolean omitCrossProd = false;

    
    public IndexPlanValidator(boolean omitCrossProd) {
        this.omitCrossProd = omitCrossProd;
    }
    
    public void setOmitCrossProd(boolean omitCrossProd) {
        this.omitCrossProd = omitCrossProd;
    }
    
    
    @Override
    public boolean isValid(TupleExpr te) {

        TupleValidateVisitor tv = new TupleValidateVisitor();
        te.visit(tv);

        return tv.isValid();
    }

    
    
   
    public int getValidTupleSize(Iterator<TupleExpr> iter) {
        
        int size = 0;
        
        while(iter.hasNext()) {
            if(isValid(iter.next())) {
                size++;
            }
        }
        
        return size;
        
    }
    
    
    
    @Override
    public Iterator<TupleExpr> getValidTuples(Iterator<TupleExpr> tupleIter) {

        final Iterator<TupleExpr> iter = tupleIter;
        
        return new Iterator<TupleExpr>() {

            private TupleExpr next = null;
            private boolean hasNextCalled = false;
            private boolean isEmpty = false;

            @Override
            public boolean hasNext() {

                if (!hasNextCalled && !isEmpty) {
                    while (iter.hasNext()) {
                        TupleExpr temp = iter.next();
                        if (isValid(temp)) {
                            next = temp;
                            hasNextCalled = true;
                            return true;
                        }
                    }
                    isEmpty = true;
                    return false;
                } else if(isEmpty) {
                    return false;
                }else {
                    return true;
                }
            }

            @Override
            public TupleExpr next() {

                if (hasNextCalled) {
                    hasNextCalled = false;
                    return next;
                } else if(isEmpty) {
                    throw new NoSuchElementException();
                }else {
                    if (this.hasNext()) {
                        hasNextCalled = false;
                        return next;
                    } else {
                        throw new NoSuchElementException();
                    }
                }
            }

            @Override
            public void remove() {

                throw new UnsupportedOperationException("Cannot delete from iterator!");

            }

        };
    }

    private boolean isJoinValid(Join join) {

        Set<String> leftBindingNames = join.getLeftArg().getBindingNames();
        Set<String> rightBindingNames = join.getRightArg().getBindingNames();

        
        //System.out.println("Left binding names are " + leftBindingNames + " and right binding names are " + rightBindingNames);
        
        if (Sets.intersection(leftBindingNames, rightBindingNames).size() == 0) {
            if (omitCrossProd) {
                return false;
            } else {
                return true;
            }

        } else {
            if (join.getRightArg() instanceof ExternalTupleSet) {

                return ((ExternalTupleSet) join.getRightArg()).supportsBindingSet(leftBindingNames);

            } else {
                return true;
            }
        }

    }

    public class TupleValidateVisitor extends QueryModelVisitorBase<RuntimeException> {

        private boolean isValid = true;

        public boolean isValid() {
            return isValid;
        }

        @Override
        public void meet(Projection node) {
            node.getArg().visit(this);
        }

        @Override
        public void meet(StatementPattern node) {
            return;
        }

        public void meet(BindingSetAssignment node) {
            return;
        }

        @Override
        public void meet(Filter node) {
            node.getArg().visit(this);
        }

        @Override
        public void meet(Join node) {
            if (isJoinValid(node)) {
                super.meet(node);
            } else {
                isValid = false;
                return;
            }
        }

    }

}
