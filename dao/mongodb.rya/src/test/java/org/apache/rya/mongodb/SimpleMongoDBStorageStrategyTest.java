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
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.apache.rya.mongodb.document.util.DocumentVisibilityConversionException;
import org.apache.rya.mongodb.document.util.DocumentVisibilityUtil;
import org.apache.rya.mongodb.document.visibility.DocumentVisibility;
import org.bson.Document;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Test;

import com.mongodb.MongoException;

public class SimpleMongoDBStorageStrategyTest {
    private static final String SUBJECT = "http://subject.com";
    private static final String PREDICATE = "http://temp.com";
    private static final String OBJECT = "http://object.com";
    private static final String CONTEXT = "http://context.com";
    private static final String STATEMENT_METADATA = "{}";
    private static final DocumentVisibility DOCUMENT_VISIBILITY = new DocumentVisibility("A&B");

    private static final RyaStatement testStatement;
    private static final RyaStatement testStatement2;
    private static final Document TEST_DOC;
    private static final Document TEST_DOC_2;
    private final SimpleMongoDBStorageStrategy storageStrategy = new SimpleMongoDBStorageStrategy();

    static {
        RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setPredicate(new RyaIRI(PREDICATE));
        builder.setSubject(new RyaIRI(SUBJECT));
        builder.setObject(new RyaIRI(OBJECT));
        builder.setContext(new RyaIRI(CONTEXT));
        builder.setColumnVisibility(DOCUMENT_VISIBILITY.flatten());
        builder.setTimestamp(null);
        testStatement = builder.build();

        TEST_DOC = new Document();
        TEST_DOC.put(SimpleMongoDBStorageStrategy.ID, "d5f8fea0e85300478da2c9b4e132c69502e21221");
        TEST_DOC.put(SimpleMongoDBStorageStrategy.SUBJECT, SUBJECT);
        TEST_DOC.put(SimpleMongoDBStorageStrategy.SUBJECT_HASH, DigestUtils.sha256Hex(SUBJECT));
        TEST_DOC.put(SimpleMongoDBStorageStrategy.PREDICATE, PREDICATE);
        TEST_DOC.put(SimpleMongoDBStorageStrategy.PREDICATE_HASH, DigestUtils.sha256Hex(PREDICATE));
        TEST_DOC.put(SimpleMongoDBStorageStrategy.OBJECT, OBJECT);
        TEST_DOC.put(SimpleMongoDBStorageStrategy.OBJECT_HASH, DigestUtils.sha256Hex(OBJECT));
        TEST_DOC.put(SimpleMongoDBStorageStrategy.OBJECT_TYPE, ANYURI.stringValue());
        TEST_DOC.put(SimpleMongoDBStorageStrategy.OBJECT_LANGUAGE, null);
        TEST_DOC.put(SimpleMongoDBStorageStrategy.CONTEXT, CONTEXT);
        TEST_DOC.put(SimpleMongoDBStorageStrategy.STATEMENT_METADATA, STATEMENT_METADATA);
        try {
            TEST_DOC.put(SimpleMongoDBStorageStrategy.DOCUMENT_VISIBILITY, DocumentVisibilityUtil.toMultidimensionalArray(DOCUMENT_VISIBILITY));
        } catch (final DocumentVisibilityConversionException e) {
            e.printStackTrace();
        }
        TEST_DOC.put(SimpleMongoDBStorageStrategy.TIMESTAMP, null);


        builder = new RyaStatementBuilder();
        builder.setPredicate(new RyaIRI(PREDICATE));
        builder.setSubject(new RyaIRI(SUBJECT));
        builder.setObject(new RyaType(RDF.LANGSTRING, OBJECT, "en-US"));
        builder.setContext(new RyaIRI(CONTEXT));
        builder.setColumnVisibility(DOCUMENT_VISIBILITY.flatten());
        builder.setTimestamp(null);
        testStatement2 = builder.build();

        // Check language support
        TEST_DOC_2 = new Document();
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.ID, "580fb5d11f0b62fa735ac98b36bba1fc37ddc3fc");
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.SUBJECT, SUBJECT);
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.SUBJECT_HASH, DigestUtils.sha256Hex(SUBJECT));
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.PREDICATE, PREDICATE);
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.PREDICATE_HASH, DigestUtils.sha256Hex(PREDICATE));
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.OBJECT, OBJECT);
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.OBJECT_HASH, DigestUtils.sha256Hex(OBJECT));
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.OBJECT_TYPE, RDF.LANGSTRING.stringValue());
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.OBJECT_LANGUAGE, "en-US");
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.CONTEXT, CONTEXT);
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.STATEMENT_METADATA, STATEMENT_METADATA);
        try {
            TEST_DOC_2.put(SimpleMongoDBStorageStrategy.DOCUMENT_VISIBILITY, DocumentVisibilityUtil.toMultidimensionalArray(DOCUMENT_VISIBILITY));
        } catch (final DocumentVisibilityConversionException e) {
            e.printStackTrace();
        }
        TEST_DOC_2.put(SimpleMongoDBStorageStrategy.TIMESTAMP, null);
    }

    @Test
    public void testSerializeStatementToDocument() throws RyaDAOException, MongoException, IOException {

        Document doc = storageStrategy.serialize(testStatement);
        assertEquals(TEST_DOC, doc);

        doc = storageStrategy.serialize(testStatement2);
        assertEquals(TEST_DOC_2, doc);
    }

    @Test
    public void testDeSerializeStatementToDocument() throws RyaDAOException, MongoException, IOException {
        RyaStatement statement = storageStrategy.deserializeDocument(TEST_DOC);
        /*
         * Since RyaStatement creates a timestamp using JVM time if the timestamp is null, we want to re-null it
         * for this test.  Timestamp is created at insert time by the Server, this test
         * can be found in the RyaDAO.
         */
        statement.setTimestamp(null);
        assertEquals(testStatement, statement);

        statement = storageStrategy.deserializeDocument(TEST_DOC_2);
        /*
         * Since RyaStatement creates a timestamp using JVM time if the timestamp is null, we want to re-null it
         * for this test.  Timestamp is created at insert time by the Server, this test
         * can be found in the RyaDAO.
         */
        statement.setTimestamp(null);
        assertEquals(testStatement2, statement);
    }
}
