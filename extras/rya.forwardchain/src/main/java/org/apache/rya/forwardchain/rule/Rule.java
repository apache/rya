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

import java.util.Collection;

import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.forwardchain.ForwardChainException;
import org.apache.rya.forwardchain.strategy.AbstractRuleExecutionStrategy;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

/**
 * Represents a forward-chaining inference rule. A rule is triggered by some
 * combination of triples, and may produce some combination of triples when
 * applied. Potential triggers (antecedents) and potential results (consequents)
 * are represented in a general form as {@link StatementPattern}s and can be
 * used to determine relationships between rules.
 */
public interface Rule {
    /**
     * Whether this rule, if applied, could produce triples of a given form.
     * @param sp A statement pattern describing a possible inferred triple;
     *  assumed not null.
     * @return true if a consequent of this rule could match the pattern.
     */
    abstract public boolean canConclude(StatementPattern sp);

    /**
     * All {@link StatementPattern}s that can, in some combination, trigger this
     * rule. Should be a complete set, such that if no statements matching any
     * of the patterns exist, the rule cannot derive any new information.
     * @return Any number of statement patterns.
     */
    abstract public Collection<StatementPattern> getAntecedentPatterns();

    /**
     * {@link StatementPattern}s completely describing the possible conclusions
     * of this rule. Any derived statement should match one of these patterns.
     * @return Any number of statement patterns.
     */
    abstract public Collection<StatementPattern> getConsequentPatterns();

    /**
     * Given an {@link AbstractRuleExecutionStrategy}, executes this rule.
     * Associates any new or modified triples with the specified statement
     * metadata.
     * @param strategy A strategy capable of applying individual rules; should
     *  not be null.
     * @param metadata StatementMetadata to add to any results. Can be used to
     *  record the circumstances of the derivation. Should not be null; use
     *  {@link StatementMetadata#EMPTY_METADATA} to add none. Implementing
     *  classes may add additional metadata specific to the rule.
     * @return The number of new inferences made during rule execution.
     * @throws ForwardChainException if an error was encountered during
     *  rule application.
     */
    abstract public long execute(AbstractRuleExecutionStrategy strategy,
            StatementMetadata metadata) throws ForwardChainException;
}
