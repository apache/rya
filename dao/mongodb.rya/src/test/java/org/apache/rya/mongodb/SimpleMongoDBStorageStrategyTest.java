package org.apache.rya.mongodb;
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

import static org.junit.Assert.assertEquals;
import static org.openrdf.model.vocabulary.XMLSchema.ANYURI;

import java.io.IOException;

import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;

public class SimpleMongoDBStorageStrategyTest {
    private static final String SUBJECT = "http://subject.com";
    private static final String PREDICATE = "http://temp.com";
    private static final String OBJECT = "http://object.com";
    private static final String CONTEXT = "http://context.com";

    private static final RyaStatement testStatement;
    private static final DBObject testDBO;
    private final SimpleMongoDBStorageStrategy storageStrategy = new SimpleMongoDBStorageStrategy();

    static {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setPredicate(new RyaURI(PREDICATE));
        builder.setSubject(new RyaURI(SUBJECT));
        builder.setObject(new RyaURI(OBJECT));
        builder.setContext(new RyaURI(CONTEXT));
        builder.setTimestamp(null);
        testStatement = builder.build();

        testDBO = new BasicDBObject();
        testDBO.put("_id", "d5f8fea0e85300478da2c9b4e132c69502e21221");
        testDBO.put("subject", SUBJECT);
        testDBO.put("predicate", PREDICATE);
        testDBO.put("object", OBJECT);
        testDBO.put("objectType", ANYURI.stringValue());
        testDBO.put("context", CONTEXT);
        testDBO.put("insertTimestamp", null);
    }

    @Test
    public void testSerializeStatementToDBO() throws RyaDAOException, MongoException, IOException {

        final DBObject dbo = storageStrategy.serialize(testStatement);
        assertEquals(testDBO, dbo);
    }

    @Test
    public void testDeSerializeStatementToDBO() throws RyaDAOException, MongoException, IOException {
        final RyaStatement statement = storageStrategy.deserializeDBObject(testDBO);
        /**
         * Since RyaStatement creates a timestamp using JVM time if the timestamp is null, we want to re-null it
         * for this test.  Timestamp is created at insert time by the Server, this test
         * can be found in the RyaDAO.
         */
        statement.setTimestamp(null);
        assertEquals(testStatement, statement);
    }
}
