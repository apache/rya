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

import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.forwardchain.ForwardChainConstants;
import org.apache.rya.forwardchain.ForwardChainException;
import org.apache.rya.forwardchain.rule.Rule;
import org.apache.rya.forwardchain.rule.Ruleset;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple {@link AbstractForwardChainStrategy} that iterates over every
 * relevant rule once, and repeats until no rules are relevant.
 * <p>
 * Initially, all rules are considered relevant. Iteration 1 executes each rule
 * once.
 * <p>
 * When a rule produces inferences, all rules that are marked as that rule's
 * successors according to the {@link Ruleset} are triggered as potentially
 * relevant for future execution. If a triggered rule is scheduled to be
 * executed during the current iteration, nothing changes. If a triggered rule
 * has already been executed once during the current iteration, or was not
 * activated for the current iteration at all, it is flagged to be executed
 * during the next iteration.
 * <p>
 * When an iteration concludes, a new iteration begins with the relevant set of
 * rules having been determined during the previous iteration. If there are no
 * such rules, forward chaining ends.
 * <p>
 * Within each iteration, rules are processed such that a rule which may trigger
 * many other rules is given priority over a rule that may be triggered by many
 * other rules.
 * <p>
 * The observation that one rule may trigger another is based on the
 * relationships between triple patterns produced and consumed by the rules in
 * general, not based on any triples that were actually generated. Therefore,
 * there may be false positives but not false negatives: Rules triggered by the
 * current rule may or may not produce more triples in response, but any rule
 * that could produce triples in response will be triggered.
 * <p>
 * The procedure for executing the individual rules is governed by the
 * {@link AbstractRuleExecutionStrategy}. This class uses the strategy's reported counts
 * to determine whether or not a rule has produced inferences.
 */
public class RoundRobinStrategy extends AbstractForwardChainStrategy {
    private static final Logger logger = Logger.getLogger(RoundRobinStrategy.class);

    private final AbstractRuleExecutionStrategy ruleStrategy;
    private int iteration;
    private Ruleset ruleset;
    private Set<Rule> activeNow;
    private Set<Rule> activeNextIteration;
    private long inferencesThisIteration;
    private AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * Instantiate a RoundRobinStrategy by providing the RuleExecutionStrategy.
     * @param ruleStrategy Defines how to execute individual rules; not null.
     */
    public RoundRobinStrategy(AbstractRuleExecutionStrategy ruleStrategy) {
        Preconditions.checkNotNull(ruleStrategy);
        this.ruleStrategy = ruleStrategy;
    }

    @Override
    public void initialize(Ruleset withRuleset) throws ForwardChainException {
        Preconditions.checkNotNull(withRuleset);
        iteration = 0;
        ruleset = withRuleset;
        activeNow = new HashSet<>();
        activeNextIteration = new HashSet<>(ruleset.getRules());
        logger.info("Initializing round robin forward chaining, with " +
                activeNextIteration.size() + " rules.");
        initialized.set(true);
        prepareQueue();
    }

    private void prepareQueue() throws ForwardChainException {
        if (initialized.get()) {
            if (activeNow.isEmpty()) {
                if (iteration > 0) {
                    logger.info("Finished iteration " + iteration + "; made " +
                            inferencesThisIteration + " inferences.");
                }
                if (activeNextIteration.isEmpty()) {
                    logger.info("Finished forward chaining after " + iteration + " iterations.");
                    setDone();
                }
                else {
                    ruleStrategy.setRequiredLevel(iteration);
                    iteration++;
                    inferencesThisIteration = 0;
                    activeNow.addAll(activeNextIteration);
                    activeNextIteration.clear();
                    logger.info("Beginning iteration " + iteration + ", with " +
                            activeNow.size() + " rules to execute...");
                }
            }
        }
    }

    private void setDone() throws ForwardChainException {
        initialized.set(false);
        if (ruleStrategy != null) {
            ruleStrategy.shutDown();
        }
    }

    @Override
    public boolean isActive() {
        return initialized.get();
    }

    @Override
    public long executeNext() throws ForwardChainException {
        if (!initialized.get()) {
            return 0;
        }
        Rule rule = getNextRule();
        if (rule == null) {
            return 0;
        }
        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(ForwardChainConstants.RYA_DERIVATION_TIME,
                new RyaType(XMLSchema.INT, Integer.toString(iteration)));
        long inferences = rule.execute(ruleStrategy, metadata);
        inferencesThisIteration += inferences;
        if (inferences > 0) {
            for (Rule successor : ruleset.getSuccessorsOf(rule)) {
                // If we'll handle the triggered rule in the current iteration,
                // it may not need  to be checked in the next one.
                if (!activeNow.contains(successor)) {
                    activeNextIteration.add(successor);
                }
            }
        }
        prepareQueue();
        return inferences;
    }

    private Rule getNextRule() {
        if (activeNow.isEmpty()) {
            return null;
        }
        Ruleset subset = new Ruleset(activeNow);
        SortedSet<Rule> sorted = new TreeSet<>(new Comparator<Rule>() {
            @Override
            public int compare(Rule r1, Rule r2) {
                // If one rule triggers the other (directly or indirectly) but
                // not the other way around, the one that triggers the other
                // should come first.
                boolean forwardPath = subset.pathExists(r1, r2);
                boolean backwardPath = subset.pathExists(r2, r1);
                if (forwardPath && !backwardPath) {
                    return -1;
                }
                if (backwardPath && !forwardPath) {
                    return 1;
                }
                return 0;
            }
        }.thenComparingInt(rule -> {
            // Otherwise, prioritize rules that trigger many remaining rules,
            // and defer rules that can be triggered by many remaining rules.
            return remainingPredecessors(rule).size() - remainingSuccessors(rule).size();
        }).thenComparing(Rule::toString)); // Fall back on string comparison
        sorted.addAll(activeNow);
        Rule next = sorted.first();
        activeNow.remove(next);
        return next;
    }

    private Set<Rule> remainingSuccessors(Rule rule) {
        Set<Rule> successors = new HashSet<>(ruleset.getSuccessorsOf(rule));
        successors.retainAll(activeNow);
        return successors;
    }

    private Set<Rule> remainingPredecessors(Rule rule) {
        Set<Rule> predecessors = new HashSet<>(ruleset.getPredecessorsOf(rule));
        predecessors.retainAll(activeNow);
        return predecessors;
    }
}
