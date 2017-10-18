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
package org.apache.rya.indexing.mongodb;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.entity.EntityCentricIndex;
import org.apache.rya.indexing.mongodb.freetext.MongoFreeTextIndexer;
import org.apache.rya.indexing.mongodb.temporal.MongoTemporalIndexer;
import org.apache.rya.mongodb.AbstractMongoDBRdfConfigurationBuilder;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRdfConfigurationBuilder;
import org.eclipse.rdf4j.sail.Sail;

import com.google.common.base.Preconditions;

/**
 * This class is an extension of the MongoDBRdfConfiguration object used to to
 * create a {@link Sail} connection to an Mongo backed instance of Rya. This
 * configuration object is designed to create Mongo Rya Sail connections where
 * one or more of the Mongo Rya Indexes are enabled. These indexes include the
 * {@link MongoFreeTextIndexer} and {@link MongoTemporalIndexer}
 *
 */
public class MongoIndexingConfiguration extends MongoDBRdfConfiguration {

    private MongoIndexingConfiguration() {
    }

    private MongoIndexingConfiguration(final Configuration conf) {
        super(conf);
    }

    public static MongoDBIndexingConfigBuilder builder() {
        return new MongoDBIndexingConfigBuilder();
    }

    /**
     * Creates a MongoIndexingConfiguration object from a Properties file. This
     * method assumes that all values in the Properties file are Strings and
     * that the Properties file uses the keys below.
     *
     * <br>
     * <ul>
     * <li>"mongo.auths" - String of Mongo authorizations. Empty auths used by
     * default.
     * <li>"mongo.visibilities" - String of Mongo visibilities assigned to
     * ingested triples.
     * <li>"mongo.user" - Mongo user. Empty by default.
     * <li>"mongo.password" - Mongo password. Empty by default.
     * <li>"mongo.host" - Mongo host. Default host is "localhost"
     * <li>"mongo.port" - Mongo port. Default port is "27017".
     * <li>"mongo.db.name" - Name of MongoDB. Default name is "rya_triples".
     * <li>"mongo.collection.prefix" - Mongo collection prefix. Default is
     * "rya_".
     * <li>"mongo.rya.prefix" - Prefix for Mongo Rya instance. Same as value of
     * "mongo.collection.prefix".
     * <li>"use.mock" - Use a Embedded Mongo instance as back-end for Rya
     * instance. False by default.
     * <li>"use.display.plan" - Display query plan during evaluation. Useful for
     * debugging. True by default.
     * <li>"use.inference" - Use backward chaining inference during query. False
     * by default.
     * <li>"use.freetext" - Use Mongo Freetext indexer for query and ingest.
     * False by default.
     * <li>"use.temporal" - Use Mongo Temporal indexer for query and ingest.
     * False by default.
     * <li>"use.entity" - Use Mongo Entity indexer for query and ingest. False
     * by default.
     * <li>"freetext.predicates" - Freetext predicates used for ingest. Specify
     * as comma delimited Strings with no spaces between. Empty by default.
     * <li>"temporal.predicates" - Temporal predicates used for ingest. Specify
     * as comma delimited Strings with no spaces between. Empty by default.
     * </ul>
     * <br>
     *
     * @param props
     *            - Properties file containing Mongo specific configuration
     *            parameters
     * @return MongoIndexingConfiguration with properties set
     */
    public static MongoIndexingConfiguration fromProperties(final Properties props) {
        return MongoDBIndexingConfigBuilder.fromProperties(props);
    }

    /**
     *
     * Specify whether to use use {@link EntityCentricIndex} for ingest and at
     * query time. The default value is false, and if useEntity is set to true
     * and the EntityIndex does not exist, then useEntity will default to false.
     *
     * @param useEntity
     *            - use entity indexing
     */
    public void setUseEntity(final boolean useEntity) {
        setBoolean(ConfigUtils.USE_ENTITY, useEntity);
    }

    /**
     * @return boolean indicating whether the {@link EntityCentricIndex} is enabled
     */
    public boolean getUseEntity() {
        return getBoolean(ConfigUtils.USE_ENTITY, false);
    }

    /**
     *
     * Specify whether to use use {@link MongoTemporalIndexer} for ingest and
     * at query time. The default value is false, and if useTemporal is set to
     * true and the TemporalIndex does not exist, then useTemporal will default
     * to false.
     *
     * @param useTemporal
     *            - use temporal indexing
     */
    public void setUseTemporal(final boolean useTemporal) {
        setBoolean(ConfigUtils.USE_TEMPORAL, useTemporal);
    }

    /**
     * @return boolean indicating whether {@link MongoTemporalIndexer} is enabled
     */
    public boolean getUseTemporal() {
        return getBoolean(ConfigUtils.USE_TEMPORAL, false);
    }

    /**
     * @return boolean indicating whether {@link MongoFreeTextIndexer} is enabled
     */
    public boolean getUseFreetext() {
        return getBoolean(ConfigUtils.USE_FREETEXT, false);
    }

    /**
     *
     * Specify whether to use use {@link MongoFreeTextIndexer} for ingest and
     * at query time. The default value is false, and if useFreeText is set to
     * true and the FreeTextIndex does not exist, then useFreeText will default
     * to false.
     *
     * @param useFreeText
     *            - use freetext indexing
     */
    public void setUseFreetext(final boolean useFreetext) {
        setBoolean(ConfigUtils.USE_FREETEXT, useFreetext);
    }

    /**
     * Sets the predicates that the {@link MongoFreeTextIndexer} uses for indexing
     * @param predicates - predicate URI used for freetext indexing
     */
    public void setMongoFreeTextPredicates(final String[] predicates) {
        Preconditions.checkNotNull(predicates, "Freetext predicates cannot be null.");
        setStrings(ConfigUtils.FREETEXT_PREDICATES_LIST, predicates);
    }

    /**
     * @return Array of predicate URI Strings used for freetext indexing
     */
    public String[] getMongoFreeTextPredicates() {
        return getStrings(ConfigUtils.FREETEXT_PREDICATES_LIST);
    }

    /**
     * Sets the predicates that the {@link MongoTemporalIndexer} uses for indexing
     * @param predicates - predicate URI used for temporal indexing
     */
    public void setMongoTemporalPredicates(final String[] predicates) {
        Preconditions.checkNotNull(predicates, "Freetext predicates cannot be null.");
        setStrings(ConfigUtils.TEMPORAL_PREDICATES_LIST, predicates);
    }

    /**
     * Gets the predicates that the {@link MongoTemporalIndexer} uses for indexing
     * @return Array of predicate URI Strings used for temporal indexing
     */
    public String[] getMongoTemporalPredicates() {
        return getStrings(ConfigUtils.TEMPORAL_PREDICATES_LIST);
    }

    /**
     * Concrete extension of {@link AbstractMongoDBRdfConfigurationBuilder} that
     * adds setter methods to configure Mongo Rya indexers in addition the core
     * Mongo Rya configuration. This builder should be used instead of
     * {@link MongoDBRdfConfigurationBuilder} to configure a query client to use
     * one or more Mongo Indexers.
     *
     */
    public static class MongoDBIndexingConfigBuilder
            extends AbstractMongoDBRdfConfigurationBuilder<MongoDBIndexingConfigBuilder, MongoIndexingConfiguration> {

        private boolean useFreetext = false;
        private boolean useTemporal = false;
        private boolean useEntity = false;
        private String[] freetextPredicates;
        private String[] temporalPredicates;

        private static final String USE_FREETEXT = "use.freetext";
        private static final String USE_TEMPORAL = "use.temporal";
        private static final String USE_ENTITY = "use.entity";
        private static final String TEMPORAL_PREDICATES = "temporal.predicates";
        private static final String FREETEXT_PREDICATES = "freetext.predicates";

        /**
         * Creates a MongoIndexingConfiguration object from a Properties file.
         * This method assumes that all values in the Properties file are
         * Strings and that the Properties file uses the keys below.
         *
         * <br>
         * <ul>
         * <li>"mongo.auths" - String of Mongo authorizations. Empty auths used
         * by default.
         * <li>"mongo.visibilities" - String of Mongo visibilities assigned to
         * ingested triples.
         * <li>"mongo.user" - Mongo user. Empty by default.
         * <li>"mongo.password" - Mongo password. Empty by default.
         * <li>"mongo.host" - Mongo host. Default host is "localhost"
         * <li>"mongo.port" - Mongo port. Default port is "27017".
         * <li>"mongo.db.name" - Name of MongoDB. Default name is "rya_triples".
         * <li>"mongo.collection.prefix" - Mongo collection prefix. Default is
         * "rya_".
         * <li>"mongo.rya.prefix" - Prefix for Mongo Rya instance. Same as value
         * of "mongo.collection.prefix".
         * <li>"use.mock" - Use a Embedded Mongo instance as back-end for Rya
         * instance. False by default.
         * <li>"use.display.plan" - Display query plan during evaluation. Useful
         * for debugging. True by default.
         * <li>"use.inference" - Use backward chaining inference during query.
         * False by default.
         * <li>"use.freetext" - Use Mongo Freetext indexer for query and ingest.
         * False by default.
         * <li>"use.temporal" - Use Mongo Temporal indexer for query and ingest.
         * False by default.
         * <li>"use.entity" - Use Mongo Entity indexer for query and ingest.
         * False by default.
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
         *            - Properties file containing Mongo specific configuration
         *            parameters
         * @return MongoIndexingConfiguration with properties set
         */
        public static MongoIndexingConfiguration fromProperties(final Properties props) {
            try {
                final MongoDBIndexingConfigBuilder builder = new MongoDBIndexingConfigBuilder() //
                        .setAuths(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_AUTHS, "")) //
                        .setRyaPrefix(
                                props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_RYA_PREFIX, "rya_"))//
                        .setVisibilities(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_VISIBILITIES, ""))
                        .setUseInference(getBoolean(
                                props.getProperty(AbstractMongoDBRdfConfigurationBuilder.USE_INFERENCE, "false")))//
                        .setDisplayQueryPlan(getBoolean(props
                                .getProperty(AbstractMongoDBRdfConfigurationBuilder.USE_DISPLAY_QUERY_PLAN, "true")))//
                        .setMongoUser(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_USER)) //
                        .setMongoPassword(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_PASSWORD))//
                        .setMongoCollectionPrefix(props
                                .getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_COLLECTION_PREFIX, "rya_"))//
                        .setMongoDBName(
                                props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_DB_NAME, "rya_triples"))//
                        .setMongoHost(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_HOST, "localhost"))//
                        .setMongoPort(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_PORT,
                                AbstractMongoDBRdfConfigurationBuilder.DEFAULT_MONGO_PORT))//
                        .setUseMockMongo(getBoolean(
                                props.getProperty(AbstractMongoDBRdfConfigurationBuilder.USE_MOCK_MONGO, "false")))//
                        .setUseMongoFreetextIndex(getBoolean(props.getProperty(USE_FREETEXT, "false")))//
                        .setUseMongoTemporalIndex(getBoolean(props.getProperty(USE_TEMPORAL, "false")))//
                        .setUseMongoEntityIndex(getBoolean(props.getProperty(USE_ENTITY, "false")))//
                        .setMongoFreeTextPredicates(StringUtils.split(props.getProperty(FREETEXT_PREDICATES), ","))//
                        .setMongoTemporalPredicates(StringUtils.split(props.getProperty(TEMPORAL_PREDICATES), ","));

                return builder.build();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         *
         * Specify whether to use use {@link MongoFreeTextIndexer} for ingest
         * and at query time. The default value is false, and if useFreeText is
         * set to true and the FreeTextIndex does not exist, then useFreeText
         * will default to false.
         *
         * @param useFreeText
         *            - use freetext indexing
         * @return MongoIndexingConfigBuilder for chaining method invocations
         */
        public MongoDBIndexingConfigBuilder setUseMongoFreetextIndex(final boolean useFreeText) {
            this.useFreetext = useFreeText;
            return this;
        }

        /**
         *
         * Specify whether to use use {@link MongoTemporalIndexer} for ingest
         * and at query time. The default value is false, and if useTemporal is
         * set to true and the TemporalIndex does not exist, then useTemporal
         * will default to false.
         *
         * @param useTemporal
         *            - use temporal indexing
         * @return MongoIndexingConfigBuilder for chaining method invocations
         */
        public MongoDBIndexingConfigBuilder setUseMongoTemporalIndex(final boolean useTemporal) {
            this.useTemporal = useTemporal;
            return this;
        }

        /**
         *
         * Specify whether to use the MongoEntityIndexer for ingest and at query
         * time. The default value is false, and if useEntity is set to true and
         * the EntityIndex does not exist, then useEntity will default to false.
         *
         * @param useEntity
         *            - use entity indexing
         * @return MongoIndexingConfigBuilder for chaining method invocations
         */
        public MongoDBIndexingConfigBuilder setUseMongoEntityIndex(final boolean useEntity) {
            this.useEntity = useEntity;
            return this;
        }

        /**
         *
         * @param predicates
         *            - String of comma delimited predicates used by the
         *            FreetextIndexer to determine which triples to index
         * @return MongoIndexingConfigBuilder for chaining method invocations
         */
        public MongoDBIndexingConfigBuilder setMongoFreeTextPredicates(final String... predicates) {
            this.freetextPredicates = predicates;
            return this;
        }

        /**
         *
         * @param predicates
         *            - String of comma delimited predicates used by the
         *            TemporalIndexer to determine which triples to index
         * @return MongoIndexingConfigBuilder for chaining method invocations
         */
        public MongoDBIndexingConfigBuilder setMongoTemporalPredicates(final String... predicates) {
            this.temporalPredicates = predicates;
            return this;
        }

        /**
         * @return {@link MongoIndexingConfiguration} with specified parameters set
         */
        @Override
        public MongoIndexingConfiguration build() {
            final MongoIndexingConfiguration conf = getConf(super.build());
            return conf;
        }

        /**
         * Assigns builder values to appropriate parameters within the {@link Configuration} object.
         *
         * @param conf - Configuration object
         * @return - Configuration object with parameters set
         */
        private MongoIndexingConfiguration getConf(final MongoIndexingConfiguration conf) {

            if (useFreetext) {
                conf.setUseFreetext(useFreetext);
                if (freetextPredicates != null) {
                    conf.setMongoFreeTextPredicates(freetextPredicates);
                }
            }
            if (useTemporal) {
                conf.setUseTemporal(useTemporal);
                if (temporalPredicates != null) {
                    conf.setMongoTemporalPredicates(temporalPredicates);
                }
            }

            conf.setUseEntity(useEntity);

            return conf;
        }

        @Override
        protected MongoDBIndexingConfigBuilder confBuilder() {
            return this;
        }

        @Override
        protected MongoIndexingConfiguration createConf() {
            return new MongoIndexingConfiguration();
        }

    }
}
