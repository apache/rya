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
package org.apache.rya.rdftriplestore.utils;

import java.util.ArrayList;
import java.util.Collection;

import org.openrdf.model.Statement;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

/**
 * StatementPattern gives fixed statements back
 *
 * Class FixedStatementPattern
 * Date: Mar 12, 2012
 * Time: 2:42:06 PM
 */
public class FixedStatementPattern extends StatementPattern {
    public Collection<Statement> statements = new ArrayList<>();

    public FixedStatementPattern() {
    }

    public FixedStatementPattern(final Var subject, final Var predicate, final Var object) {
        super(subject, predicate, object);
    }

    public FixedStatementPattern(final Scope scope, final Var subject, final Var predicate, final Var object) {
        super(scope, subject, predicate, object);
    }

    public FixedStatementPattern(final Var subject, final Var predicate, final Var object, final Var context) {
        super(subject, predicate, object, context);
    }

    public FixedStatementPattern(final Scope scope, final Var subjVar, final Var predVar, final Var objVar, final Var conVar) {
        super(scope, subjVar, predVar, objVar, conVar);
    }
}
