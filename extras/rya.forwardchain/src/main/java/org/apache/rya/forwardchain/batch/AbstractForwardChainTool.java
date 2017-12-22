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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.forwardchain.ForwardChainException;
import org.apache.rya.forwardchain.rule.Ruleset;
import org.apache.rya.forwardchain.strategy.AbstractForwardChainStrategy;
import org.apache.rya.forwardchain.strategy.AbstractRuleExecutionStrategy;
import org.apache.rya.forwardchain.strategy.MongoPipelineStrategy;
import org.apache.rya.forwardchain.strategy.RoundRobinStrategy;
import org.apache.rya.forwardchain.strategy.SailExecutionStrategy;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import com.google.common.base.Preconditions;

/**
 * Base class for a {@link Tool} that executes forward-chaining rules until
 * completion (when no more new information can be derived).
 * <p>
 * Subclasses must implement {@link #getRuleset()} to yield the specific set of
 * {@link Rule}s to materialize.
 * <p>
 * Subclasses may additionally override {@link #getStrategy()} and/or
 * {@link #getRuleStrategy()} to provide specific forward chaining execution
 * logic.
 */
public abstract class AbstractForwardChainTool implements Tool {
    private static final Logger logger = Logger.getLogger(AbstractForwardChainTool.class);

    private RdfCloudTripleStoreConfiguration conf;

    private long numInferences = 0;

    /**
     * Set the {@link Configuration} for this tool, which will be converted to
     * an {@link RdfCloudTripleStoreConfiguration}.
     * @param conf Configuration object that specifies Rya connection details.
     *  Should not be null.
     */
    @Override
    public void setConf(Configuration conf) {
        Preconditions.checkNotNull(conf);
        if (conf.getBoolean(ConfigUtils.USE_MONGO, false)) {
            this.conf = new MongoDBRdfConfiguration(conf);
        }
        else {
            this.conf = new AccumuloRdfConfiguration(conf);
        }
    }

    /**
     * Get the RdfCloudTripleStoreConfiguration used by this tool.
     * @return Rya configuration object.
     */
    @Override
    public RdfCloudTripleStoreConfiguration getConf() {
        return conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        numInferences = getStrategy().executeAll(getRuleset());
        logger.info("Forward chaining complete; made " + numInferences + " inferences.");
        return 0;
    }

    /**
     * Gets the number of inferences that have been made.
     * @return zero before forward chaining, or the total number of inferences
     *  after.
     */
    public long getNumInferences() {
        return numInferences;
    }

    /**
     * Get the high-level {@link AbstractForwardChainStrategy} that governs how
     * reasoning will proceed. By default, returns a {@link RoundRobinStrategy}
     * which executes each relevant rule one-by-one, then moves to the next
     * iteration and repeats, until no rules are still relevant. Subclasses may
     * override this method to provide alternative strategies.
     * @return The high-level forward chaining logic.
     * @throws ForwardChainException if the strategy can't be instantiated.
     */
    protected AbstractForwardChainStrategy getStrategy() throws ForwardChainException {
        return new RoundRobinStrategy(getRuleStrategy());
    }

    /**
     * Get the low-level {@link AbstractRuleExecutionStrategy} that governs the
     * application of rules on an individual basis. This is used by the default
     * ForwardChainStrategy (RoundRobinStrategy) and may be used by any
     * high-level strategy that executes rules individually. By default, returns
     * a {@link MongoPipelineStrategy} if the configuration object specifies a
     * MongoDB connection with aggregation pipelines enabled, and a
     * {@link SailExecutionStrategy} otherwise. Subclasses may override this
     * method to provide alternative strategies.
     * @return The low-level rule execution logic.
     * @throws ForwardChainExceptionthe strategy can't be instantiated.
     */
    protected AbstractRuleExecutionStrategy getRuleStrategy() throws ForwardChainException {
        if (ConfigUtils.getUseMongo(conf)) {
            final MongoDBRdfConfiguration mongoConf;
            if (conf instanceof MongoDBRdfConfiguration) {
                mongoConf = (MongoDBRdfConfiguration) conf;
            }
            else {
                mongoConf = new MongoDBRdfConfiguration(conf);
            }
            if (mongoConf.getUseAggregationPipeline()) {
                return new MongoPipelineStrategy(mongoConf);
            }
        }
        return new SailExecutionStrategy(conf);
    }

    /**
     * Get the set of rules for this tool to apply. Subclasses should implement
     * this for their specific domains. The subclass should ensure that the
     * ruleset returned only contains rules whose types are supported by the
     * forward chaining strategy. The default strategy supports only CONSTRUCT
     * rules, so the ruleset should only contain {@link AbstractConstructRule}s.
     * @return A set of forward-chaining rules.
     * @throws ForwardChainException if rules couldn't be retrieved.
     */
    protected abstract Ruleset getRuleset() throws ForwardChainException;
}
