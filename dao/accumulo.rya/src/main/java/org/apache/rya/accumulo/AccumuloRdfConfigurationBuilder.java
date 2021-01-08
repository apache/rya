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
 */package org.apache.rya.accumulo;

import java.util.Properties;

/**
 * This is a concrete extension of the
 * {@link AbstractAccumuloRdfConfigurationBuilder} class which builds an
 * {@link AccumuloRdfConfiguration} object. This builder creates an
 * AccumuloRdfConfiguratio object and sets all of the parameters required to
 * connect to an Accumulo Rya instance.
 *
 */
public class AccumuloRdfConfigurationBuilder
        extends AbstractAccumuloRdfConfigurationBuilder<AccumuloRdfConfigurationBuilder, AccumuloRdfConfiguration> {

    /**
     * Creates an AccumuloRdfConfiguration object from a Properties file. This
     * method assumes that all values in the Properties file are Strings and
     * that the Properties file uses the keys below. See
     * accumulo/rya/src/test/resources/properties/rya.properties for an example.
     * <br>
     * <ul>
     * <li>"accumulo.auths" - String of Accumulo authorizations. Default is
     * empty String.
     * <li>"accumulo.visibilities" - String of Accumulo visibilities assigned to
     * ingested triples.
     * <li>"accumulo.instance" - Accumulo instance name (required)
     * <li>"accumulo.user" - Accumulo user (required)
     * <li>"accumulo.password" - Accumulo password (required)
     * <li>"accumulo.rya.prefix" - Prefix for Accumulo backed Rya instance.
     * Default is "rya_"
     * <li>"accumulo.zookeepers" - Zookeepers for underlying Accumulo instance
     * (required if not using Mock)
     * <li>"use.mock" - Use a MockAccumulo instance as back-end for Rya
     * instance. Default is false.
     * <li>"use.prefix.hashing" - Use prefix hashing for triples. Helps avoid
     * hot-spotting. Default is false.
     * <li>"use.count.stats" - Use triple pattern cardinalities for query
     * optimization. Default is false.
     * <li>"use.join.selectivity" - Use join selectivity for query optimization.
     * Default is false.
     * <li>"use.display.plan" - Display query plan during evaluation. Useful for
     * debugging. Default is true.
     * <li>"use.inference" - Use backward chaining inference during query
     * evaluation. Default is false.
     * </ul>
     * <br>
     * 
     * @param props
     *            - Properties file containing Accumulo specific configuration
     *            parameters
     * @return AccumumuloRdfConfiguration with properties set
     */
    public static AccumuloRdfConfigurationBuilder fromProperties(Properties props) {
        AccumuloRdfConfigurationBuilder builder = new AccumuloRdfConfigurationBuilder()
                .setUseMockAccumulo(getBoolean(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_MOCK_ACCUMULO, "false")))
                .setAccumuloInstance(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_INSTANCE))
                .setAccumuloZooKeepers(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_ZOOKEEPERS))
                .setAccumuloUser(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_USER))
                .setAccumuloPassword(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_PASSWORD))
                .setRyaPrefix(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_RYA_PREFIX, "rya_"))
                .setAuths(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_AUTHS, ""))
                .setVisibilities(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_VISIBILITIES, ""))
                .setUseAccumuloPrefixHashing(getBoolean(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_PREFIX_HASHING, "false")))
                .setUseInference(getBoolean(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_INFERENCE, "false")))
                .setDisplayQueryPlan(getBoolean(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_DISPLAY_QUERY_PLAN, "true")))
                .setUseCompositeCardinality(getBoolean(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_COUNT_STATS, "false")))
                .setUseJoinSelectivity(getBoolean(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_JOIN_SELECTIVITY, "false")));
        return builder;
    }

    @Override
    protected AccumuloRdfConfigurationBuilder confBuilder() {
        return this;
    }

    @Override
    protected AccumuloRdfConfiguration createConf() {
        return new AccumuloRdfConfiguration();
    }

}
