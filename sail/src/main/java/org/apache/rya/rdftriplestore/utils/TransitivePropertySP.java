package org.apache.rya.rdftriplestore.utils;

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



import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

/**
 * Class TransitivePropertySP
 * Date: Mar 14, 2012
 * Time: 5:23:10 PM
 */
public class TransitivePropertySP extends StatementPattern {

    public TransitivePropertySP() {
    }

    public TransitivePropertySP(Var subject, Var predicate, Var object) {
        super(subject, predicate, object);
    }

    public TransitivePropertySP(Scope scope, Var subject, Var predicate, Var object) {
        super(scope, subject, predicate, object);
    }

    public TransitivePropertySP(Var subject, Var predicate, Var object, Var context) {
        super(subject, predicate, object, context);
    }

    public TransitivePropertySP(Scope scope, Var subjVar, Var predVar, Var objVar, Var conVar) {
        super(scope, subjVar, predVar, objVar, conVar);
    }
}
