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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern;

import com.google.common.base.Preconditions;

/**
 * Represents a set of forward-chaining {@link Rule}s and their relationships.
 */
public class Ruleset {
    private final Set<Rule> rules;
    private final Map<Rule, Set<Rule>> successors;
    private final Map<Rule, Set<Rule>> predecessors;

    private final Logger logger = Logger.getLogger(this.getClass());

    /**
     * Constructor. Takes in a set of rules and determines their dependencies.
     * @param rules The complete set of rules to process; should not be null.
     */
    public Ruleset(Collection<Rule> rules) {
        Preconditions.checkNotNull(rules);
        this.rules = new HashSet<>();
        for (Rule rule : rules) {
            if (rule != null) {
                this.rules.add(rule);
            }
        }
        successors = new ConcurrentHashMap<>();
        predecessors = new ConcurrentHashMap<>();
        // Build the dependency graph of all the rules, in both directions
        for (Rule rule : rules) {
            successors.put(rule, new HashSet<>());
            predecessors.put(rule, new HashSet<>());
        }
        for (Rule rule1 : rules) {
            for (Rule rule2 : rules) {
                if (canTrigger(rule1, rule2)) {
                    logger.trace("\t" + rule1.toString() + " can trigger " + rule2.toString());
                    successors.get(rule1).add(rule2);
                    predecessors.get(rule2).add(rule1);
                }
            }
        }
    }

    /**
     * Get the rules associated with this ruleset.
     * @return The complete set of rules.
     */
    public Set<Rule> getRules() {
        return rules;
    }

    /**
     * Given a rule, return the set of all rules that it may trigger. That is,
     * if the rule were to produce inferences, those inferences might directly
     * cause other rules to apply in turn.
     * @param precedingRule The potentially triggering rule; not null.
     * @return All rules that could be triggered by the given rule.
     */
    public Collection<Rule> getSuccessorsOf(Rule precedingRule) {
        Preconditions.checkNotNull(precedingRule);
        return successors.get(precedingRule);
    }

    /**
     * Given a rule, return the set of all rules that could trigger it. That is,
     * if any one of those rules were applied, their potential conclusions could
     * directly cause the specified rule to apply in turn.
     * @param dependentRule The potentially triggered rule; not null.
     * @return All rules that could trigger the given rule.
     */
    public Collection<Rule> getPredecessorsOf(Rule dependentRule) {
        Preconditions.checkNotNull(dependentRule);
        return predecessors.get(dependentRule);
    }

    /**
     * Given a pair of rules, determine whether a path exists from the first to
     * the second. That is, whether the first rule precedes the second rule
     * either directly or transitively. If either rule is null, no path exists.
     * @param r1 The start of the path
     * @param r2 The end of the path
     * @return whether a forward path exists.
     */
    public boolean pathExists(Rule r1, Rule r2) {
        if (r1 == null || r2 == null) {
            return false;
        }
        Set<Rule> forwardFrontier = new HashSet<>();
        Set<Rule> backwardFrontier = new HashSet<>();
        Set<Rule> visitedForward = new HashSet<>();
        Set<Rule> visitedBackward = new HashSet<>();
        forwardFrontier.addAll(getSuccessorsOf(r1));
        backwardFrontier.add(r2);
        while (!forwardFrontier.isEmpty() && !backwardFrontier.isEmpty()) {
            Set<Rule> currentGoals = new HashSet<>(backwardFrontier);
            for (Rule goal : currentGoals) {
                if (forwardFrontier.contains(goal)) {
                    return true;
                }
                else {
                    visitedBackward.add(goal);
                    backwardFrontier.addAll(getPredecessorsOf(goal));
                }
            }
            backwardFrontier.removeAll(visitedBackward);
            Set<Rule> currentSources = new HashSet<>(forwardFrontier);
            for (Rule source : currentSources) {
                if (backwardFrontier.contains(source)) {
                    return true;
                }
                else {
                    visitedForward.add(source);
                    forwardFrontier.addAll(getSuccessorsOf(source));
                }
            }
            forwardFrontier.removeAll(visitedForward);
        }
        return false;
    }

    /**
     * Whether the first rule can, in any circumstance, directly trigger the second.
     * @param rule1 The first rule, which may produce some inferences
     * @param rule2 The second rule, which may use the first rule's conclusions
     * @return True if the first rule's conclusions could be used by the second.
     */
    private boolean canTrigger(Rule rule1, Rule rule2) {
        if (rule1 == null || rule2 == null) {
            return false;
        }
        for (StatementPattern antecedent : rule2.getAntecedentPatterns()) {
            if (rule1.canConclude(antecedent)) {
                return true;
            }
        }
        return false;
    }
}
