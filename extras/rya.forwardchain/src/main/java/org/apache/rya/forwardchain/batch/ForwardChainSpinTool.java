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
package org.apache.rya.forwardchain.batch;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.forwardchain.ForwardChainException;
import org.apache.rya.forwardchain.rule.Ruleset;
import org.apache.rya.forwardchain.rule.SpinConstructRule;

/**
 * {@link Tool} to load SPIN Construct rules from a Rya data store, then apply
 * those rules to the same store using forward-chaining inference
 * (materialization), adding triples back to Rya until no more information can
 * be derived.
 */
public class ForwardChainSpinTool extends AbstractForwardChainTool {
    private Ruleset ruleset;

    /**
     * Constructor that takes in an {@link RdfCloudTripleStoreConfiguration}.
     * @param conf Configuration object containing Rya connection information.
     */
    public ForwardChainSpinTool(RdfCloudTripleStoreConfiguration conf) {
        setConf(conf);
    }

    /**
     * Default constructor that does not take in a configuration object. Rya
     * connection details should be provided via an
     * RdfCloudTripleStoreConfiguration, either using
     * {@link AbstractForwardChainTool#setConf} or a {@link ToolRunner}.
     */
    public ForwardChainSpinTool() { }

    /**
     * Load SPIN Construct rules from Rya.
     * @return A set of construct query rules.
     * @throws ForwardChainException if loading rules from Rya fails.
     */
    @Override
    protected Ruleset getRuleset() throws ForwardChainException {
        if (ruleset == null) {
            ruleset = SpinConstructRule.loadSpinRules(getConf());
        }
        return ruleset;
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        ForwardChainSpinTool tool = new ForwardChainSpinTool();
        ToolRunner.run(tool, args);
        long end = System.currentTimeMillis();
        double seconds = (end - start) / 1000.0;
        long inferences = tool.getNumInferences();
        long rules = tool.getRuleset().getRules().size();
        System.out.println(String.format("ForwardChainSpinTool: %d rules, %d inferences, %.3f seconds",
                rules, inferences, seconds));
    }
}
