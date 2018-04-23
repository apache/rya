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

import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.forwardchain.ForwardChainException;
import org.apache.rya.forwardchain.strategy.AbstractRuleExecutionStrategy;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;

import com.google.common.base.Preconditions;

/**
 * A rule that produces new triples, and can be expressed as a graph query
 * (SPARQL "CONSTRUCT"). Should not modify existing triples.
 */
public abstract class AbstractConstructRule implements Rule {
    /**
     * Get the query tree corresponding to this construct rule.
     * @return The query algebra representation of this rule.
     */
    public abstract ParsedGraphQuery getQuery();

    @Override
    public long execute(AbstractRuleExecutionStrategy strategy,
            StatementMetadata metadata) throws ForwardChainException {
        Preconditions.checkNotNull(strategy);
        Preconditions.checkNotNull(metadata);
        return strategy.executeConstructRule(this, metadata);
    }

    /**
     * Whether any of the possible consequents of this rule include anonymous
     * variables. Care should be taken when executing such rules, so that
     * repeated application doesn't continually produce new bnodes.
     * @return true if any subject, predicate, or object variable involved in a
     *      consequent is flagged as anonymous.
     */
    public boolean hasAnonymousConsequent() {
        for (StatementPattern sp : getConsequentPatterns()) {
            if (sp.getSubjectVar().isAnonymous()
                    || sp.getPredicateVar().isAnonymous()
                    || sp.getObjectVar().isAnonymous()) {
                return true;
            }
        }
        return false;
    }
}
