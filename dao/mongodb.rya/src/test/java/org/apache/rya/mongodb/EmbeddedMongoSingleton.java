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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;

import de.flapdoodle.embed.mongo.config.IMongodConfig;

/**
 * To be used for tests. Creates a singleton {@link MongoClient} to be used
 * throughout all of the MongoDB related tests. Without the singleton, the
 * embedded mongo factory ends up orphaning processes, consuming resources.
 */
public class EmbeddedMongoSingleton {
    public static MongoClient getInstance() {
        return InstanceHolder.SINGLETON.instance;
    }

    public static IMongodConfig getMongodConfig() {
        return InstanceHolder.SINGLETON.mongodConfig;
    }

    private EmbeddedMongoSingleton() {
        // hiding implicit default constructor
    }

    private enum InstanceHolder {

        SINGLETON;

        private final Logger log;
        private MongoClient instance;
        private IMongodConfig mongodConfig;
        InstanceHolder() {
            log = LoggerFactory.getLogger(EmbeddedMongoSingleton.class);
            instance = null;
            try {
                EmbeddedMongoFactory factory = EmbeddedMongoFactory.newFactory();
                instance = factory.newMongoClient();
                mongodConfig = factory.getMongoServerDetails();
                // JUnit does not have an overall lifecycle event for tearing down
                // this kind of resource, but shutdown hooks work alright in practice
                // since this should only be used during testing

                // The only other alternative for lifecycle management is to use a
                // suite lifecycle to enclose the tests that need this resource.
                // In practice this becomes unwieldy.
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        try {
                            instance.close();
                        } catch (final Throwable t) {
                            // logging frameworks will likely be shut down
                            t.printStackTrace(System.err);
                        }
                    }
                });

            } catch (final IOException e) {
                log.error("Unexpected error while starting mongo client", e);
            } catch (final Throwable e) {
                // catching throwable because failure to construct an enum
                // instance will lead to another error being thrown downstream
                log.error("Unexpected throwable while starting mongo client", e);
            }
        }
    }
}
