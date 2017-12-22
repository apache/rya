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
package org.apache.rya.forwardchain.strategy;

import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.forwardchain.ForwardChainException;

import org.apache.rya.forwardchain.rule.AbstractConstructRule;
import org.apache.rya.forwardchain.rule.AbstractInconsistencyRule;
import org.apache.rya.forwardchain.rule.AbstractUpdateRule;

/**
 * Base class for rule application strategies, which can execute a single
 * forward chaining (materialization) rule at a time. Subclasses may provide
 * implementations of methods to execute whichever they support of construct
 * rules, update rules, and inconsistency rules. The default behavior for all
 * kinds is to throw an {@link UnsupportedOperationException}.
 */
public abstract class AbstractRuleExecutionStrategy {
    protected int requiredLevel = 0;

    /**
     * Execute a rule corresponding to a "CONSTRUCT" query. Throws an
     * UnsupportedOperationException if not explicitly overridden.
     * @param rule The construct rule to apply; assumed not null.
     * @param metadata Additional metadata to add to any inferred triples;
     *  assumed not null.
     * @return The number of inferred triples. Higher-level forward chaining
     *  strategies may rely on the accuracy of this number.
     * @throws ForwardChainException if execution failed.
     */
    public long executeConstructRule(AbstractConstructRule rule,
            StatementMetadata metadata) throws ForwardChainException {
        throw new UnsupportedOperationException("Rule execution strategy does not support construct rules.");
    };

    /**
     * Execute a rule corresponding to an update query. Throws an
     * UnsupportedOperationException if not explicitly overridden.
     * @param rule The update rule to apply; assumed not null.
     * @param metadata Additional metadata to add to any updated triples;
     *  assumed not null.
     * @return The number of inferences made. Higher-level forward chaining
     *  strategies may rely on the accuracy of this number.
     * @throws ForwardChainException if execution failed.
     */
    public long executeUpdateRule(AbstractUpdateRule rule,
            StatementMetadata metadata) throws ForwardChainException {
        throw new UnsupportedOperationException("Rule execution strategy does not support update rules.");
    };

    /**
     * Execute a rule capable of detecting inconsistencies. Throws an
     * UnsupportedOperationException if not explicitly overridden.
     * @param rule The inconsistency rule to apply; assumed not null.
     * @param metadata Additional metadata associated with inconsistencies;
     *  assumed not null.
     * @return The number of inconsistencies found.
     * @throws ForwardChainException if execution failed.
     */
    public long executeInconsistencyRule(AbstractInconsistencyRule rule,
            StatementMetadata metadata) throws ForwardChainException {
        throw new UnsupportedOperationException("Rule execution strategy does not perform inconsistency detection.");
    }

    /**
     * Initialize the strategy and make any preparations for executing rules.
     * Does nothing by default; subclasses should override if necessary.
     * @throws ForwardChainException
     */
    public void initialize() throws ForwardChainException { };

    /**
     * Shut down the strategy and perform any appropriate cleanup. Does nothing
     * by default; subclasses should override if necessary.
     * @throws ForwardChainException
     */
    public void shutDown() throws ForwardChainException { }

    /**
     * Indicate that a rule need only be applied if one of the source statements
     * is is at least this derivation level, i.e. took this many steps to derive
     * itself. Subclasses may use this for optimization, but are not guaranteed
     * to.
     * @param derivationLevel Forward chaining level of statements that should
     *  be used to trigger rules. If not set, defaults to zero which should have
     *  no effect.
     */
    public void setRequiredLevel(int derivationLevel) {
        this.requiredLevel = derivationLevel;
    };
}
