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
package org.apache.rya.mongodb;

import java.util.Properties;

/**
 * This is a concrete extension of the
 * {@link AbstractMongoDBRdfConfigurationBuilder} class which builds an
 * {@link MongoDBRdfConfiguration} object. This builder creates an
 * MongoDBRdfConfiguratio object and sets all of the parameters required to
 * connect to an Mongo Rya instance.
 *
 */
public class MongoDBRdfConfigurationBuilder
        extends AbstractMongoDBRdfConfigurationBuilder<MongoDBRdfConfigurationBuilder, MongoDBRdfConfiguration> {

    /**
     * Creates a MongoRdfConfiguration object from a Properties file. This
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
     * </ul>
     * <br>
     * 
     * @param props
     *            - Properties file containing Mongo specific configuration
     *            parameters
     * @return MongoRdfConfiguration with properties set
     */
    public static MongoDBRdfConfiguration fromProperties(Properties props) {
        try {

            MongoDBRdfConfigurationBuilder builder = new MongoDBRdfConfigurationBuilder()
                    .setRyaPrefix(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_RYA_PREFIX, "rya_"))
                    .setAuths(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_AUTHS, ""))
                    .setVisibilities(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_VISIBILITIES, ""))
                    .setUseInference(getBoolean(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.USE_INFERENCE, "false")))
                    .setDisplayQueryPlan(getBoolean(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.USE_DISPLAY_QUERY_PLAN, "true")))
                    .setMongoUser(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_USER))
                    .setMongoPassword(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_PASSWORD))
                    .setMongoCollectionPrefix(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_COLLECTION_PREFIX, "rya_"))
                    .setMongoDBName(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_DB_NAME, "rya_triples"))
                    .setMongoHost(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_HOST, "localhost"))
                    .setMongoPort(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.MONGO_PORT, AbstractMongoDBRdfConfigurationBuilder.DEFAULT_MONGO_PORT))
                    .setUseMockMongo(getBoolean(props.getProperty(AbstractMongoDBRdfConfigurationBuilder.USE_MOCK_MONGO, "false")));

            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected MongoDBRdfConfigurationBuilder confBuilder() {
        return this;
    }

    @Override
    protected MongoDBRdfConfiguration createConf() {
        return new MongoDBRdfConfiguration();
    }

}
