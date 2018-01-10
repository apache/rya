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

import org.apache.rya.forwardchain.ForwardChainException;
import org.apache.rya.forwardchain.rule.Ruleset;

/**
 * Base class for high-level strategies which define how to conduct
 * forward-chaining reasoning (materialization).
 */
public abstract class AbstractForwardChainStrategy {
    /**
     * A running count of new inferences so far.
     */
    protected long totalInferences;

    /**
     * Initializes reasoning with respect to a given ruleset.
     * @param ruleset The complete set of rules to materialize. Should not be
     *  null.
     * @throws ForwardChainException if initialization fails.
     */
    abstract public void initialize(Ruleset ruleset) throws ForwardChainException;

    /**
     * Whether forward chaining is both initialized and yet to finish.
     * @return true if a ruleset has been provided and some rules may still
     *  yield new information.
     */
    abstract protected boolean isActive();

    /**
     * Execute the next step of reasoning, such as a single rule if the strategy
     * proceeds one rule at a time.
     * @return The number of inferences made during this step.
     * @throws ForwardChainException if any error is encountered during rule
     *  application.
     */
    abstract protected long executeNext() throws ForwardChainException;

    /**
     * Execute an entire ruleset until no new rules can be derived. Initializes
     * strategy and proceeds until completion.
     * @param rules The complete set of rules; not null.
     * @return The number of total inferences made.
     * @throws ForwardChainException if any error is encountered during
     *  initialization or application.
     */
    public long executeAll(Ruleset rules) throws ForwardChainException {
        initialize(rules);
        totalInferences = 0;
        while (isActive()) {
            totalInferences += executeNext();
        }
        return totalInferences;
    }

    /**
     * Get the running total of inferences made so far.
     * @return The number of inferences made since initialization.
     */
    public long getNumInferences() {
        return totalInferences;
    }
}
