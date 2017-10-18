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
package org.apache.rya.indexing.accumulo;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AbstractAccumuloRdfConfigurationBuilder;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRdfConfigurationBuilder;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.accumulo.entity.EntityCentricIndex;
import org.apache.rya.indexing.accumulo.freetext.AccumuloFreeTextIndexer;
import org.apache.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer;
import org.apache.rya.indexing.external.PrecomputedJoinIndexer;
import org.eclipse.rdf4j.sail.Sail;

/**
 * This class is an extension of the AccumuloRdfConfiguration object used to to
 * create a {@link Sail} connection to an Accumulo backed instance of Rya. This
 * configuration object is designed to create Accumulo Rya Sail connections
 * where one or more of the Accumulo Rya Indexes are enabled. These indexes
 * include the {@link AccumuloFreeTextIndexer}, {@link AccumuloTemporalIndexer},
 * {@link EntityCentricIndex}, and the {@link PrecomputedJoinIndexer}.
 *
 */
public class AccumuloIndexingConfiguration extends AccumuloRdfConfiguration {

    private AccumuloIndexingConfiguration() {
    }

    public static AccumuloIndexingConfigBuilder builder() {
        return new AccumuloIndexingConfigBuilder();
    }

    /**
     * Creates an AccumuloIndexingConfiguration object from a Properties file.
     * This method assumes that all values in the Properties file are Strings
     * and that the Properties file uses the keys below.
     * 
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
     * <li>"use.freetext" - Use Accumulo Freetext Indexer for querying and
     * ingest. Default is false.
     * <li>"use.temporal" - Use Accumulo Temporal Indexer for querying and
     * ingest. Default is false.
     * <li>"use.entity" - Use Accumulo Entity Index for querying and ingest.
     * Default is false.
     * <li>"fluo.app.name" - Set name of Fluo App to update PCJs.
     * <li>"use.pcj" - Use PCJs for query optimization. Default is false.
     * <li>"use.optimal.pcj" - Use optimal PCJ for query optimization. Default
     * is false.
     * <li>"pcj.tables" - PCJ tables to be used, specified as comma delimited
     * Strings with no spaces between. If no tables are specified, all
     * registered tables are used.
     * <li>"freetext.predicates" - Freetext predicates used for ingest. Specify
     * as comma delimited Strings with no spaces between. Empty by default.
     * <li>"temporal.predicates" - Temporal predicates used for ingest. Specify
     * as comma delimited Strings with no spaces between. Empty by default.
     * </ul>
     * <br>
     * 
     * @param props
     *            - Properties file containing Accumulo specific configuration
     *            parameters
     * @return AccumumuloIndexingConfiguration with properties set
     */
    public static AccumuloIndexingConfiguration fromProperties(Properties props) {
        return AccumuloIndexingConfigBuilder.fromProperties(props);
    }

    /**
     * 
     * Specify whether to use use {@link EntitCentricIndex} for ingest and at
     * query time. The default value is false, and if useEntity is set to true
     * and the EntityIndex does not exist, then useEntity will default to false.
     * 
     * @param useEntity
     *            - use entity indexing
     */
    public void setUseEntity(boolean useEntity) {
        setBoolean(ConfigUtils.USE_ENTITY, useEntity);
    }

    /**
     * @return boolean indicating whether or not {@link EntityCentricIndex} is enabled
     */
    public boolean getUseEntity() {
        return getBoolean(ConfigUtils.USE_ENTITY, false);
    }

    /**
     * 
     * Specify whether to use use {@link AccumuloTemproalIndexer} for ingest and
     * at query time. The default value is false, and if useTemporal is set to
     * true and the TemporalIndex does not exist, then useTemporal will default
     * to false.
     * 
     * @param useTemporal
     *            - use temporal indexing
     */
    public void setUseTemporal(boolean useTemporal) {
        setBoolean(ConfigUtils.USE_TEMPORAL, useTemporal);
    }

    /**
     * @return boolean indicating whether or not {@link AccumuloTemporalIndexer} is enabled
     */
    public boolean getUseTemporal() {
        return getBoolean(ConfigUtils.USE_TEMPORAL, false);
    }

    /**
     * @return boolean indicating whether or not {@link AccumuloFreeTextIndexer} is enabled
     */
    public boolean getUseFreetext() {
        return getBoolean(ConfigUtils.USE_FREETEXT, false);
    }

    /**
     * 
     * Specify whether to use use {@link AccumuloFreeTextIndexer} for ingest and
     * at query time. The default value is false, and if useFreeText is set to
     * true and the FreeTextIndex does not exist, then useFreeText will default
     * to false.
     * 
     * @param useFreeText
     *            - use freetext indexing
     */
    public void setUseFreetext(boolean useFreetext) {
        setBoolean(ConfigUtils.USE_FREETEXT, useFreetext);
    }

    /**
     * @return boolean indicating whether or not {@link PrecomputedJoinIndexer} is enabled
     */
    public boolean getUsePCJUpdater() {
        return getBoolean(ConfigUtils.USE_PCJ_UPDATER_INDEX, false);
    }

    public void setUsePCJUpdater(boolean usePCJUpdater) {
        setBoolean(ConfigUtils.USE_PCJ_UPDATER_INDEX, usePCJUpdater);
        if (usePCJUpdater) {
            set(ConfigUtils.PCJ_STORAGE_TYPE, "ACCUMULO");
            set(ConfigUtils.PCJ_UPDATER_TYPE, "FLUO");
        }
    }

    /**
     * 
     * Specify the name of the PCJ Fluo updater application. A non-null
     * application results in the {@link PrecomputedJoinIndexer} being activated
     * so that all triples ingested into Rya are also ingested into Fluo to
     * update any registered PCJs. PreomputedJoinIndexer is turned off by
     * default. If no fluo application of the specified name exists, a
     * RuntimeException will occur.
     * 
     * @param fluoAppName
     *            - use entity indexing
     */
    public void setFluoAppUpdaterName(String fluoAppName) {
        Preconditions.checkNotNull(fluoAppName, "Fluo app name cannot be null.");
        setUsePCJUpdater(true);
        set(ConfigUtils.FLUO_APP_NAME, fluoAppName);
    }

    /**
     * @return name of the Fluo PCJ Updater application
     */
    public String getFluoAppUpdaterName() {
        return get(ConfigUtils.FLUO_APP_NAME);
    }

    /**
     * Use Precomputed Joins as a query optimization.
     * 
     * @param usePcj
     *            - use PCJ
     */
    public void setUsePCJ(boolean usePCJ) {
        setBoolean(ConfigUtils.USE_PCJ, usePCJ);
    }

    /**
     * @return boolean indicating whether or not PCJs are enabled for querying
     */
    public boolean getUsePCJ() {
        return getBoolean(ConfigUtils.USE_PCJ, false);
    }

    /**
     * Use Precomputed Joins as a query optimization and attempt to find the
     * best combination of PCJ in the query plan
     * 
     * @param useOptimalPcj
     *            - use optimal pcj plan
     */
    public void setUseOptimalPCJ(boolean useOptimalPCJ) {
        setBoolean(ConfigUtils.USE_OPTIMAL_PCJ, useOptimalPCJ);
    }

    /**
     * @return boolean indicating whether or not query planner will look for optimal
     * combinations of PCJs when forming the query plan.
     */
    public boolean getUseOptimalPCJ() {
        return getBoolean(ConfigUtils.USE_OPTIMAL_PCJ, false);
    }

    /**
     * Sets the predicates used for freetext indexing
     * @param predicates - Array of predicate URI strings used for freetext indexing
     */
    public void setAccumuloFreeTextPredicates(String[] predicates) {
        Preconditions.checkNotNull(predicates, "Freetext predicates cannot be null.");
        setStrings(ConfigUtils.FREETEXT_PREDICATES_LIST, predicates);
    }

    /**
     * Gets the predicates used for freetext indexing
     * @return Array of predicate URI strings used for freetext indexing
     */
    public String[] getAccumuloFreeTextPredicates() {
        return getStrings(ConfigUtils.FREETEXT_PREDICATES_LIST);
    }

    /**
     * Sets the predicates used for temporal indexing
     * @param predicates - Array of predicate URI strings used for temporal indexing
     */
    public void setAccumuloTemporalPredicates(String[] predicates) {
        Preconditions.checkNotNull(predicates, "Freetext predicates cannot be null.");
        setStrings(ConfigUtils.TEMPORAL_PREDICATES_LIST, predicates);
    }

    /**
     * Gets the predicates used for temporal indexing
     * @return Array of predicate URI strings used for temporal indexing
     */
    public String[] getAccumuloTemporalPredicates() {
        return getStrings(ConfigUtils.TEMPORAL_PREDICATES_LIST);
    }
    
    private static Set<RyaURI> getPropURIFromStrings(String ... props) {
        Set<RyaURI> properties = new HashSet<>();
        for(String prop: props) {
            properties.add(new RyaURI(prop));
        }
        return properties;
    }
    
    /**
     * Concrete extension of {@link AbstractAccumuloRdfConfigurationBuilder}
     * that adds setter methods to configure Accumulo Rya Indexers in addition
     * the core Accumulo Rya configuration. This builder should be used instead
     * of {@link AccumuloRdfConfigurationBuilder} to configure a query client to
     * use one or more Accumulo Indexers.
     *
     */
    public static class AccumuloIndexingConfigBuilder extends
            AbstractAccumuloRdfConfigurationBuilder<AccumuloIndexingConfigBuilder, AccumuloIndexingConfiguration> {

        private String fluoAppName;
        private boolean useFreetext = false;
        private boolean useTemporal = false;
        private boolean useEntity = false;
        private boolean useMetadata = false;
        private String[] freetextPredicates;
        private String[] temporalPredicates;
        private boolean usePcj = false;
        private boolean useOptimalPcj = false;
        private String[] pcjs = new String[0];
        private Set<RyaURI> metadataProps = new HashSet<>();

        private static final String USE_FREETEXT = "use.freetext";
        private static final String USE_TEMPORAL = "use.temporal";
        private static final String USE_ENTITY = "use.entity";
        private static final String FLUO_APP_NAME = "fluo.app.name";
        private static final String USE_PCJ = "use.pcj";
        private static final String USE_OPTIMAL_PCJ = "use.optimal.pcj";
        private static final String TEMPORAL_PREDICATES = "temporal.predicates";
        private static final String FREETEXT_PREDICATES = "freetext.predicates";
        private static final String PCJ_TABLES = "pcj.tables";
        private static final String USE_STATEMENT_METADATA = "use.metadata";
        private static final String STATEMENT_METADATA_PROPERTIES = "metadata.properties";

        /**
         * Creates an AccumuloIndexingConfiguration object from a Properties
         * file. This method assumes that all values in the Properties file are
         * Strings and that the Properties file uses the keys below.
         * 
         * <br>
         * <ul>
         * <li>"accumulo.auths" - String of Accumulo authorizations. Default is
         * empty String.
         * <li>"accumulo.visibilities" - String of Accumulo visibilities
         * assigned to ingested triples.
         * <li>"accumulo.instance" - Accumulo instance name (required)
         * <li>"accumulo.user" - Accumulo user (required)
         * <li>"accumulo.password" - Accumulo password (required)
         * <li>"accumulo.rya.prefix" - Prefix for Accumulo backed Rya instance.
         * Default is "rya_"
         * <li>"accumulo.zookeepers" - Zookeepers for underlying Accumulo
         * instance (required if not using Mock)
         * <li>"use.mock" - Use a MockAccumulo instance as back-end for Rya
         * instance. Default is false.
         * <li>"use.prefix.hashing" - Use prefix hashing for triples. Helps
         * avoid hot-spotting. Default is false.
         * <li>"use.count.stats" - Use triple pattern cardinalities for query
         * optimization. Default is false.
         * <li>"use.join.selectivity" - Use join selectivity for query
         * optimization. Default is false.
         * <li>"use.display.plan" - Display query plan during evaluation. Useful
         * for debugging. Default is true.
         * <li>"use.inference" - Use backward chaining inference during query
         * evaluation. Default is false.
         * <li>"use.freetext" - Use Accumulo Freetext Indexer for querying and
         * ingest. Default is false.
         * <li>"use.temporal" - Use Accumulo Temporal Indexer for querying and
         * ingest. Default is false.
         * <li>"use.entity" - Use Accumulo Entity Index for querying and ingest.
         * Default is false.
         * <li>"use.metadata" - Use Accumulo StatementMetadata index for querying Statement Properties.
         * Default is false.
         * <li>"metadata.properties" - Set Statement Properties that can be queried using the StatementMetadataOptimizer.
         * Default is empty.
         * <li>"fluo.app.name" - Set name of Fluo App to update PCJs
         * <li>"use.pcj" - Use PCJs for query optimization. Default is false.
         * <li>"use.optimal.pcj" - Use optimal PCJ for query optimization.
         * Default is false.
         * <li>"pcj.tables" - PCJ tables to be used, specified as comma
         * delimited Strings with no spaces between. If no tables are specified,
         * all registered tables are used.
         * <li>"freetext.predicates" - Freetext predicates used for ingest.
         * Specify as comma delimited Strings with no spaces between. Empty by
         * default.
         * <li>"temporal.predicates" - Temporal predicates used for ingest.
         * Specify as comma delimited Strings with no spaces between. Empty by
         * default.
         * </ul>
         * <br>
         * 
         * @param props
         *            - Properties file containing Accumulo specific
         *            configuration parameters
         * @return AccumumuloIndexingConfiguration with properties set
         */
        public static AccumuloIndexingConfiguration fromProperties(Properties props) {
            Preconditions.checkNotNull(props);
            try {
                AccumuloIndexingConfigBuilder builder = new AccumuloIndexingConfigBuilder() //
                        .setAuths(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_AUTHS, "")) //
                        .setRyaPrefix(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_RYA_PREFIX, "rya_"))//
                        .setVisibilities(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_VISIBILITIES, ""))
                        .setUseInference(getBoolean(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_INFERENCE, "false")))//
                        .setDisplayQueryPlan(getBoolean(props
                                .getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_DISPLAY_QUERY_PLAN, "true")))//
                        .setAccumuloUser(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_USER)) //
                        .setAccumuloInstance(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_INSTANCE))//
                        .setAccumuloZooKeepers(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_ZOOKEEPERS))//
                        .setAccumuloPassword(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_PASSWORD))//
                        .setUseMockAccumulo(getBoolean(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_MOCK_ACCUMULO, "false")))//
                        .setUseAccumuloPrefixHashing(getBoolean(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_PREFIX_HASHING, "false")))//
                        .setUseCompositeCardinality(getBoolean(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_COUNT_STATS, "false")))//
                        .setUseJoinSelectivity(getBoolean(props
                                .getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_JOIN_SELECTIVITY, "false")))//
                        .setUseAccumuloFreetextIndex(getBoolean(props.getProperty(USE_FREETEXT, "false")))//
                        .setUseAccumuloTemporalIndex(getBoolean(props.getProperty(USE_TEMPORAL, "false")))//
                        .setUseAccumuloEntityIndex(getBoolean(props.getProperty(USE_ENTITY, "false")))//
                        .setAccumuloFreeTextPredicates(props.getProperty(FREETEXT_PREDICATES))//
                        .setAccumuloTemporalPredicates(props.getProperty(TEMPORAL_PREDICATES))//
                        .setUsePcj(getBoolean(props.getProperty(USE_PCJ, "false")))//
                        .setUseOptimalPcj(getBoolean(props.getProperty(USE_OPTIMAL_PCJ, "false")))//
                        .setPcjTables(props.getProperty(PCJ_TABLES))//
                        .setPcjUpdaterFluoAppName(props.getProperty(FLUO_APP_NAME))
                        .setUseStatementMetadata(getBoolean(props.getProperty(USE_STATEMENT_METADATA)))
                        .setStatementMetadataProperties(getPropURIFromStrings(props.getProperty(STATEMENT_METADATA_PROPERTIES)));

                return builder.build();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * 
         * Specify whether to use use {@link AccumuloFreeTextIndexer} for ingest
         * and at query time. The default value is false, and if useFreeText is
         * set to true and the FreeTextIndex does not exist, then useFreeText
         * will default to false.
         * 
         * @param useFreeText
         *            - use freetext indexing
         * @return AccumuloIndexingConfigBuilder for chaining method invocations
         */
        public AccumuloIndexingConfigBuilder setUseAccumuloFreetextIndex(boolean useFreeText) {
            this.useFreetext = useFreeText;
            return this;
        }

        /**
         * 
         * Specify whether to use use {@link AccumuloTemporalIndexer} for ingest
         * and at query time. The default value is false, and if useTemporal is
         * set to true and the TemporalIndex does not exist, then useTemporal
         * will default to false.
         * 
         * @param useTemporal
         *            - use temporal indexing
         * @return AccumuloIndexingConfigBuilder for chaining method invocations
         */
        public AccumuloIndexingConfigBuilder setUseAccumuloTemporalIndex(boolean useTemporal) {
            this.useTemporal = useTemporal;
            return this;
        }

        /**
         * 
         * Specify whether to use use {@link EntitCentricIndex} for ingest and
         * at query time. The default value is false, and if useEntity is set to
         * true and the EntityIndex does not exist, then useEntity will default
         * to false.
         * 
         * @param useEntity
         *            - use entity indexing
         * @return AccumuloIndexingConfigBuilder for chaining method invocations
         */
        public AccumuloIndexingConfigBuilder setUseAccumuloEntityIndex(boolean useEntity) {
            this.useEntity = useEntity;
            return this;
        }

        /**
         * 
         * Specify the name of the PCJ Fluo updater application. A non-null
         * application results in the {@link PrecomputedJoinIndexer} being
         * activated so that all triples ingested into Rya are also ingested
         * into Fluo to update any registered PCJs. PreomputedJoinIndexer is
         * turned off by default. If no fluo application of the specified name
         * exists, a RuntimeException will be thrown.
         * 
         * @param fluoAppName
         *            - use entity indexing
         * @return AccumuloIndexingConfigBuilder for chaining method invocations
         */
        public AccumuloIndexingConfigBuilder setPcjUpdaterFluoAppName(String fluoAppName) {
            this.fluoAppName = fluoAppName;
            return this;
        }

        /**
         * 
         * @param predicates
         *            - String of comma delimited predicates used by the
         *            FreetextIndexer to determine which triples to index
         * @return AccumuloIndexingConfigBuilder for chaining method invocations
         */
        public AccumuloIndexingConfigBuilder setAccumuloFreeTextPredicates(String... predicates) {
            this.freetextPredicates = predicates;
            return this;
        }

        /**
         * 
         * @param predicates
         *            - String of comma delimited predicates used by the
         *            TemporalIndexer to determine which triples to index
         * @return AccumuloIndexingConfigBuilder for chaining method invocations
         */
        public AccumuloIndexingConfigBuilder setAccumuloTemporalPredicates(String... predicates) {
            this.temporalPredicates = predicates;
            return this;
        }

        /**
         * Use Precomputed Joins as a query optimization.
         * 
         * @param usePcj
         *            - use PCJ
         * @return AccumuloIndexingConfigBuilder for chaining method invocations
         */
        public AccumuloIndexingConfigBuilder setUsePcj(boolean usePcj) {
            this.usePcj = usePcj;
            return this;
        }

        /**
         * Use Precomputed Joins as a query optimization and attempt to find the
         * best combination of PCJs in the query plan
         * 
         * @param useOptimalPcj
         *            - use optimal pcj plan
         * @return AccumuloIndexingConfigBuilder for chaining method invocations
         */
        public AccumuloIndexingConfigBuilder setUseOptimalPcj(boolean useOptimalPcj) {
            this.useOptimalPcj = useOptimalPcj;
            return this;
        }

        /**
         * Specify a collection of PCJ tables to use for query optimization. If
         * no tables are specified and PCJs are enabled for query evaluation,
         * then all registered PCJs will be considered when optimizing the
         * query.
         * 
         * @param pcjs
         *            - array of PCJs to be used for query evaluation
         * @return AccumuloIndexingConfigBuilder for chaining method invocations
         */
        public AccumuloIndexingConfigBuilder setPcjTables(String... pcjs) {
            this.pcjs = pcjs;
            return this;
        }
        
        /**
         * Specify whether or not to use {@link StatementMetadataOptimizer} to query on Statement
         * properties.
         * @param useMetadata
         * @return AccumuloIndexingConfigBuilder for chaining method invocations
         */
        public AccumuloIndexingConfigBuilder setUseStatementMetadata(boolean useMetadata) {
            this.useMetadata = useMetadata;
            return this;
        }
        
        /**
         * Specify properties that the {@link StatementMetadataOptimizer} will use to query
         * @param useMetadata
         * @return AccumuloIndexingConfigBuilder for chaining method invocations
         */
        public AccumuloIndexingConfigBuilder setStatementMetadataProperties(Set<RyaURI> metadataProps) {
            this.metadataProps = metadataProps;
            return this;
        }
        
        
        /**
         * @return {@link AccumuloIndexingConfiguration} object with specified parameters set
         */
        public AccumuloIndexingConfiguration build() {
            AccumuloIndexingConfiguration conf = getConf(super.build());

            return conf;
        }

        /**
         * Assigns builder values to appropriate parameters within the {@link Configuration} object.
         * 
         * @param conf - Configuration object
         * @return - Configuration object with parameters set
         */
        private AccumuloIndexingConfiguration getConf(AccumuloIndexingConfiguration conf) {

            Preconditions.checkNotNull(conf);

            if (fluoAppName != null) {
                conf.setFluoAppUpdaterName(fluoAppName);
            }
            if (useFreetext) {
                conf.setUseFreetext(useFreetext);
                if (freetextPredicates != null) {
                    conf.setAccumuloFreeTextPredicates(freetextPredicates);
                }
            }
            if (useTemporal) {
                conf.setUseTemporal(useTemporal);
                if (temporalPredicates != null) {
                    conf.setAccumuloTemporalPredicates(temporalPredicates);
                }
            }

            if (usePcj || useOptimalPcj) {
                conf.setUsePCJ(usePcj);
                conf.setUseOptimalPCJ(useOptimalPcj);
                if (pcjs.length > 1 || (pcjs.length == 1 && pcjs[0] != null)) {
                    conf.setPcjTables(Lists.newArrayList(pcjs));
                }
            }
            
            if(useMetadata) {
                conf.setUseStatementMetadata(useMetadata);
                conf.setStatementMetadataProperties(metadataProps);
            }

            conf.setBoolean(ConfigUtils.USE_ENTITY, useEntity);

            return conf;
        }

        @Override
        protected AccumuloIndexingConfigBuilder confBuilder() {
            return this;
        }

        @Override
        protected AccumuloIndexingConfiguration createConf() {
            return new AccumuloIndexingConfiguration();
        }

    }

}
