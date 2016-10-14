package org.apache.rya.indexing;

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


import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import com.google.common.collect.Sets;

public class IndexingExpr {

    private final URI function;
    private final Value[] arguments;
    private final StatementPattern spConstraint;

    public IndexingExpr(URI function, StatementPattern spConstraint, Value... arguments) {
        this.function = function;
        this.arguments = arguments;
        this.spConstraint = spConstraint;
    }

    public URI getFunction() {
        return function;
    }

    public Value[] getArguments() {
        return arguments;
    }

    public StatementPattern getSpConstraint() {
        return spConstraint;
    }


    public Set<String> getBindingNames() {
        //resource and match variable for search are already included as standard result-bindings
        Set<String> bindings = Sets.newHashSet();

        for(Var v: spConstraint.getVarList()) {
            if(!v.isConstant()) {
                bindings.add(v.getName());
            }
        }
        return bindings;
    }
    
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IndexingExpr)) {
            return false;
        }
        IndexingExpr arg = (IndexingExpr) other;
        return (this.function.equals(arg.function)) && (this.spConstraint.equals(arg.spConstraint))
                && (this.arguments.equals(arg.arguments));
    }
    
    
    @Override
    public int hashCode() {
        int result = 17;
        result = 31*result + function.hashCode();
        result = 31*result + spConstraint.hashCode();
        result = 31*result + arguments.hashCode();
        
        return result;
    }
    
}

    

