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

import static org.eclipse.rdf4j.model.vocabulary.XMLSchema.ANYURI;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.apache.rya.mongodb.document.util.DocumentVisibilityConversionException;
import org.apache.rya.mongodb.document.util.DocumentVisibilityUtil;
import org.apache.rya.mongodb.document.visibility.DocumentVisibility;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public class SimpleMongoDBStorageStrategyTest {
    private static final String SUBJECT = "http://subject.com";
    private static final String PREDICATE = "http://temp.com";
    private static final String OBJECT = "http://object.com";
    private static final String CONTEXT = "http://context.com";
    private static final String STATEMENT_METADATA = "{}";
    private static final DocumentVisibility DOCUMENT_VISIBILITY = new DocumentVisibility("A&B");

    private static final RyaStatement testStatement;
    private static final DBObject testDBO;
    private final SimpleMongoDBStorageStrategy storageStrategy = new SimpleMongoDBStorageStrategy();

    static {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setPredicate(new RyaIRI(PREDICATE));
        builder.setSubject(new RyaIRI(SUBJECT));
        builder.setObject(new RyaIRI(OBJECT));
        builder.setContext(new RyaIRI(CONTEXT));
        builder.setColumnVisibility(DOCUMENT_VISIBILITY.flatten());
        builder.setTimestamp(null);
        testStatement = builder.build();

        testDBO = new BasicDBObject();
        testDBO.put(SimpleMongoDBStorageStrategy.ID, "d5f8fea0e85300478da2c9b4e132c69502e21221");
        testDBO.put(SimpleMongoDBStorageStrategy.SUBJECT, SUBJECT);
        testDBO.put(SimpleMongoDBStorageStrategy.SUBJECT_HASH, DigestUtils.sha256Hex(SUBJECT));
        testDBO.put(SimpleMongoDBStorageStrategy.PREDICATE, PREDICATE);
        testDBO.put(SimpleMongoDBStorageStrategy.PREDICATE_HASH, DigestUtils.sha256Hex(PREDICATE));
        testDBO.put(SimpleMongoDBStorageStrategy.OBJECT, OBJECT);
        testDBO.put(SimpleMongoDBStorageStrategy.OBJECT_HASH, DigestUtils.sha256Hex(OBJECT));
        testDBO.put(SimpleMongoDBStorageStrategy.OBJECT_TYPE, ANYURI.stringValue());
        testDBO.put(SimpleMongoDBStorageStrategy.CONTEXT, CONTEXT);
        testDBO.put(SimpleMongoDBStorageStrategy.STATEMENT_METADATA, STATEMENT_METADATA);
        try {
            testDBO.put(SimpleMongoDBStorageStrategy.DOCUMENT_VISIBILITY, DocumentVisibilityUtil.toMultidimensionalArray(DOCUMENT_VISIBILITY));
        } catch (final DocumentVisibilityConversionException e) {
            e.printStackTrace();
        }
        testDBO.put(SimpleMongoDBStorageStrategy.TIMESTAMP, null);
    }

    @Test
    public void testSerializeStatementToDBO() throws RyaDAOException, MongoException, IOException {

        final DBObject dbo = storageStrategy.serialize(testStatement);
        assertEquals(testDBO, dbo);
    }

    @Test
    public void testDeSerializeStatementToDBO() throws RyaDAOException, MongoException, IOException {
        final RyaStatement statement = storageStrategy.deserializeDBObject(testDBO);
        /*
         * Since RyaStatement creates a timestamp using JVM time if the timestamp is null, we want to re-null it
         * for this test.  Timestamp is created at insert time by the Server, this test
         * can be found in the RyaDAO.
         */
        statement.setTimestamp(null);
        assertEquals(testStatement, statement);
    }
}
