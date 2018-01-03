/**
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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link MongoDBRdfConfiguration} that is used to hold onto state that is pass into Rya components that accept
 * {@link Configuration} objects.
 * <p>
 * HACK:
 * This class is part of a hack to get around how Rya uses reflection to initialize indexers, optimizers, etc.
 * Those classes have empty constructors, so they are not able to receive Mongo specific components at construction
 * time. However, they all receive a {@link Configuration} prior to initialization. If an object of this class
 * is that configuration object, then shared objects may be passed into the constructed components.
 */
@DefaultAnnotation(NonNull.class)
public class StatefulMongoDBRdfConfiguration extends MongoDBRdfConfiguration {

    private final MongoClient mongoClient;
    private List<MongoSecondaryIndex> indexers;

    /**
     * Constructs an instance of {@link StatefulMongoDBRdfConfiguration} pre-loaded with values.
     *
     * @param other - The values that will be cloned into the constructed object. (not null)
     * @param mongoClient - The {@link MongoClient} that Rya will use. (not null)
     * @param indexers - The {@link MongoSecondaryIndex}s that Rya will use. (not null)
     */
    public StatefulMongoDBRdfConfiguration(
            final Configuration other,
            final MongoClient mongoClient,
            final List<MongoSecondaryIndex> indexers) {
        super(other);
        this.mongoClient = requireNonNull(mongoClient);
        this.indexers = requireNonNull(indexers);
    }

    /**
     * Constructs an instance of {@link StatefulMongoDBRdfConfiguration} pre-loaded with values.
     *
     * @param other - The values that will be cloned into the constructed object. (not null)
     * @param mongoClient - The {@link MongoClient} that Rya will use. (not null)
     */
    public StatefulMongoDBRdfConfiguration(
            final Configuration other,
            final MongoClient mongoClient) {
        this(other, mongoClient, new ArrayList<>());
    }

    /**
     * @return The {@link MongoClient} that Rya will use.
     */
    public MongoClient getMongoClient() {
        return mongoClient;
    }

    /**
     * @param indexers - The {@link MongoSecondaryIndex}s that Rya will use. (not null)
     */
    public void setIndexers(final List<MongoSecondaryIndex> indexers) {
        this.indexers = requireNonNull(indexers);
    }

    /**
     * @return The {@link MongoSecondaryIndex}s that Rya will use.
     */
    public List<MongoSecondaryIndex> getAdditionalIndexers() {
        return indexers;
    }

    @Override
    public MongoDBRdfConfiguration clone() {
        return new StatefulMongoDBRdfConfiguration(this, mongoClient, indexers);
    }
}