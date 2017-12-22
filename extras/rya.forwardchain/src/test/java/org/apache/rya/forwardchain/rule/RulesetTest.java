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
import java.util.Set;

import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.forwardchain.ForwardChainException;
import org.apache.rya.forwardchain.strategy.AbstractRuleExecutionStrategy;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import com.google.common.collect.Sets;

public class RulesetTest {
    private static Var c(Value val) {
        Var v = new Var("-const-" + val.stringValue(), val);
        v.setAnonymous(true);
        return v;
    }

    private static class TestRule implements Rule {
        private final Collection<StatementPattern> consume;
        private final Collection<StatementPattern> produce;
        TestRule(Collection<StatementPattern> consume, Collection<StatementPattern> produce) {
            this.consume = consume;
            this.produce = produce;
        }
        @Override
        public boolean canConclude(StatementPattern sp) {
            return produce.contains(sp);
        }
        @Override
        public Collection<StatementPattern> getAntecedentPatterns() {
            return consume;
        }
        @Override
        public Collection<StatementPattern> getConsequentPatterns() {
            return produce;
        }
        @Override
        public long execute(AbstractRuleExecutionStrategy strategy,
                StatementMetadata metadata) throws ForwardChainException {
            return 0;
        }
    }

    @Test
    public void testDependencies() {
        StatementPattern genericSP = new StatementPattern(new Var("a"), new Var("b"), new Var("c"));
        StatementPattern typeSP = new StatementPattern(new Var("x"), c(RDF.TYPE), new Var("t"));
        StatementPattern scoSP = new StatementPattern(new Var("x"), c(RDFS.SUBCLASSOF), new Var("y"));
        Rule typeTriggersAny = new TestRule(
                Sets.newHashSet(typeSP),
                Sets.newHashSet(genericSP, typeSP, scoSP));
        Rule subclassTriggersType = new TestRule(
                Sets.newHashSet(scoSP),
                Sets.newHashSet(genericSP, typeSP));
        Rule anyTriggersNothing = new TestRule(
                Sets.newHashSet(genericSP),
                Sets.newHashSet());
        Set<Rule> allRules = Sets.newHashSet(anyTriggersNothing, subclassTriggersType, typeTriggersAny);
        Set<Rule> noRules = Sets.newHashSet();
        Set<Rule> produceType = Sets.newHashSet(subclassTriggersType, typeTriggersAny);
        Set<Rule> produceSubclass = Sets.newHashSet(typeTriggersAny);
        Set<Rule> produceAny = Sets.newHashSet(subclassTriggersType, typeTriggersAny);
        Set<Rule> consumeType = Sets.newHashSet(anyTriggersNothing, typeTriggersAny);
        Ruleset ruleset = new Ruleset(allRules);
        Assert.assertEquals(produceType, ruleset.getPredecessorsOf(typeTriggersAny));
        Assert.assertEquals(allRules, ruleset.getSuccessorsOf(typeTriggersAny));
        Assert.assertEquals(produceSubclass, ruleset.getPredecessorsOf(subclassTriggersType));
        Assert.assertEquals(consumeType, ruleset.getSuccessorsOf(subclassTriggersType));
        Assert.assertEquals(produceAny, ruleset.getPredecessorsOf(anyTriggersNothing));
        Assert.assertEquals(noRules, ruleset.getSuccessorsOf(anyTriggersNothing));
    }

    @Test
    public void testIndirectDependencies() {
        StatementPattern genericSP = new StatementPattern(new Var("a"), new Var("b"), new Var("c"));
        StatementPattern typeSP = new StatementPattern(new Var("x"), c(RDF.TYPE), new Var("t"));
        StatementPattern scoSP = new StatementPattern(new Var("x"), c(RDFS.SUBCLASSOF), new Var("y"));
        StatementPattern spoSP = new StatementPattern(new Var("x"), c(RDFS.SUBPROPERTYOF), new Var("y"));
        Rule typeTriggersAny = new TestRule(
                Sets.newHashSet(typeSP),
                Sets.newHashSet(genericSP, typeSP, scoSP));
        Rule subclassTriggersType = new TestRule(
                Sets.newHashSet(scoSP),
                Sets.newHashSet(genericSP, typeSP));
        Rule anyTriggersNothing = new TestRule(
                Sets.newHashSet(genericSP),
                Sets.newHashSet());
        Rule typeTriggersSubprop = new TestRule(
                Sets.newHashSet(typeSP),
                Sets.newHashSet(genericSP, spoSP));
        Set<Rule> allRules = Sets.newHashSet(anyTriggersNothing, subclassTriggersType,
                typeTriggersAny, typeTriggersSubprop);
        Ruleset ruleset = new Ruleset(allRules);
        Assert.assertTrue(ruleset.pathExists(typeTriggersAny, typeTriggersAny));
        Assert.assertTrue(ruleset.pathExists(typeTriggersAny, subclassTriggersType));
        Assert.assertTrue(ruleset.pathExists(typeTriggersAny, anyTriggersNothing));
        Assert.assertTrue(ruleset.pathExists(typeTriggersAny, typeTriggersSubprop));
        Assert.assertTrue(ruleset.pathExists(subclassTriggersType, typeTriggersAny));
        Assert.assertTrue(ruleset.pathExists(subclassTriggersType, subclassTriggersType));
        Assert.assertTrue(ruleset.pathExists(subclassTriggersType, anyTriggersNothing));
        Assert.assertTrue(ruleset.pathExists(subclassTriggersType, typeTriggersSubprop));
        Assert.assertFalse(ruleset.pathExists(anyTriggersNothing, typeTriggersAny));
        Assert.assertFalse(ruleset.pathExists(anyTriggersNothing, subclassTriggersType));
        Assert.assertFalse(ruleset.pathExists(anyTriggersNothing, anyTriggersNothing));
        Assert.assertFalse(ruleset.pathExists(anyTriggersNothing, typeTriggersSubprop));
        Assert.assertFalse(ruleset.pathExists(typeTriggersSubprop, typeTriggersAny));
        Assert.assertFalse(ruleset.pathExists(typeTriggersSubprop, subclassTriggersType));
        Assert.assertTrue(ruleset.pathExists(typeTriggersSubprop, anyTriggersNothing));
        Assert.assertFalse(ruleset.pathExists(typeTriggersSubprop, typeTriggersSubprop));
    }
}
