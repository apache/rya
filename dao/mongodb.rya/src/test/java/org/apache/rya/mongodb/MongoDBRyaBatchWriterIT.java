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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.mongodb.batch.MongoDbBatchWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.MongoClient;

/**
 * Integration tests for the {@link MongoDbBatchWriter}.
 */
public class MongoDBRyaBatchWriterIT extends MongoTestBase {
    private MongoDBRyaDAO dao;

    private static void setupLogging() {
        BasicConfigurator.configure();
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        setupLogging();
    }

    @Before
    public void setUp() throws Exception {
        conf.setBoolean("rya.mongodb.dao.flusheachupdate", false);
        conf.setInt("rya.mongodb.dao.batchwriter.size", 50_000);
        conf.setLong("rya.mongodb.dao.batchwriter.flushtime", 100L);

        final MongoClient client = super.getMongoClient();
        dao = new MongoDBRyaDAO(conf, client);
    }

    @Test
    public void testDuplicateKeys() throws Exception {
        final List<RyaStatement> statements = new ArrayList<>();
        statements.add(statement(1));
        statements.add(statement(2));
        statements.add(statement(1));
        statements.add(statement(3));
        statements.add(statement(1));
        statements.add(statement(4));
        statements.add(statement(1));
        statements.add(statement(5));
        statements.add(statement(1));
        statements.add(statement(6));

        dao.add(statements.iterator());

        dao.flush();

        Assert.assertEquals(6, getRyaCollection().count());
    }

    private static RyaURI ryaURI(final int v) {
        return new RyaURI("u:" + v);
    }

    private static RyaStatement statement(final int v) {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setPredicate(ryaURI(v));
        builder.setSubject(ryaURI(v));
        builder.setObject(ryaURI(v));
        return builder.build();
    }
}
