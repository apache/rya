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
package org.apache.rya.forwardchain.rule;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 * Query visitor that identifies all triple patterns represented as
 * {@link StatementPattern}s in a query, which therefore represent triples
 * that could potentially contribute to a solution. Considers only the statement
 * patterns themselves, i.e. the leaves of the query tree, and does not consider
 * other constraints that may restrict the set of triples that may be relevant.
 * This means relying on this analysis to determine whether a fact can be part
 * of a solution can yield false positives, but not false negatives.
 */
class AntecedentVisitor extends AbstractQueryModelVisitor<RuntimeException> {
    private Set<StatementPattern> antecedentStatementPatterns = new HashSet<>();

    /**
     * Get the StatementPatterns used by this query.
     * @return A set of patterns that can contribute to query solutions.
     */
    public Set<StatementPattern> getAntecedents() {
        return antecedentStatementPatterns;
    }

    @Override
    public void meet(StatementPattern sp) {
        antecedentStatementPatterns.add(sp.clone());
    }
}
