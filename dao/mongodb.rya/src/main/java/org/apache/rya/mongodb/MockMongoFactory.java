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
import java.net.ServerSocket;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;
import de.flapdoodle.embed.mongo.distribution.Version;

public class MockMongoFactory {
    private static Logger logger = LoggerFactory.getLogger(MockMongoFactory.class.getName());

    public static MockMongoFactory newFactory() throws IOException {
        return MockMongoFactory.with(Version.Main.PRODUCTION);
    }
    
    public static MockMongoFactory with(final IFeatureAwareVersion version) throws IOException {
        return new MockMongoFactory(version);
    }

    private final MongodExecutable mongodExecutable;
    private final MongodProcess mongodProcess;

    /**
     * Create the testing utility using the specified version of MongoDB.
     * 
     * @param version
     *            version of MongoDB.
     */
    private MockMongoFactory(final IFeatureAwareVersion version) throws IOException {
        final MongodStarter runtime = MongodStarter.getInstance(new RuntimeConfigBuilder().defaultsWithLogger(Command.MongoD, logger).build());
        mongodExecutable = runtime.prepare(newMongodConfig(version));
        mongodProcess = mongodExecutable.start();
    }

    private IMongodConfig newMongodConfig(final IFeatureAwareVersion version) throws UnknownHostException, IOException {
        Net net = new Net(findRandomOpenPortOnAllLocalInterfaces(), false);
        return new MongodConfigBuilder().version(version).net(net).build();
    }

    private int findRandomOpenPortOnAllLocalInterfaces() throws IOException {
        try (ServerSocket socket = new ServerSocket(0);) {
            return socket.getLocalPort();
        }
    }

    /**
     * Creates a new Mongo connection.
     * 
     * @throws MongoException
     * @throws UnknownHostException
     */
    public MongoClient newMongo() throws UnknownHostException, MongoException {
        return new MongoClient(new ServerAddress(mongodProcess.getConfig().net().getServerAddress(), mongodProcess.getConfig().net().getPort()));
    }

    /**
     * Cleans up the resources created by the utility.
     */
    public void shutdown() {
        mongodProcess.stop();
        mongodExecutable.stop();
    }
}
