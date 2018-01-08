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
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import de.flapdoodle.embed.mongo.config.IMongodConfig;

/**
 * To be used for tests. Creates a singleton {@link MongoClient} to be used
 * throughout all of the MongoDB related tests. Without the singleton, the
 * embedded mongo factory ends up orphaning processes, consuming resources.
 */
public class EmbeddedMongoSingleton {

    public static MongoClient getNewMongoClient() throws UnknownHostException, MongoException {
    	final MongoClient client = InstanceHolder.SINGLETON.factory.newMongoClient();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    client.close();
                } catch (final Throwable t) {
                    // logging frameworks will likely be shut down
                    t.printStackTrace(System.err);
                }
            }
        });

        return client;
    }

    /**
     * @return The singleton Mongo DB instance's server details.
     */
    public static IMongodConfig getMongodConfig() {
        return InstanceHolder.SINGLETON.mongodConfig;
    }
    
    private EmbeddedMongoSingleton() {
        // hiding implicit default constructor
    }

    private enum InstanceHolder {

        SINGLETON;

        private final Logger log;
        private IMongodConfig mongodConfig;
        private EmbeddedMongoFactory factory;

        InstanceHolder() {
            log = LoggerFactory.getLogger(EmbeddedMongoSingleton.class);
            try {
            	factory = EmbeddedMongoFactory.newFactory();
                mongodConfig = factory.getMongoServerDetails();
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