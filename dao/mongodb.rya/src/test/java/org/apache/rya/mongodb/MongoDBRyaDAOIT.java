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

import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.DOCUMENT_VISIBILITY;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQuery;
import org.apache.rya.mongodb.document.util.AuthorizationsUtil;
import org.apache.rya.mongodb.document.visibility.DocumentVisibility;
import org.bson.Document;
import org.calrissian.mango.collect.CloseableIterable;
import org.junit.Test;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDBRyaDAOIT extends MongoTestBase {

    @Override
    protected void updateConfiguration(final MongoDBRdfConfiguration conf) {
        conf.setAuths("A", "B", "C");
    }

    @Test
    public void testDeleteWildcard() throws RyaDAOException {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final RyaStatementBuilder builder = new RyaStatementBuilder();
            builder.setPredicate(new RyaURI("http://temp.com"));
            builder.setColumnVisibility(new DocumentVisibility("A").flatten());
            dao.delete(builder.build(), conf);
        } finally {
            dao.destroy();
        }
    }

    @Test
    public void testAdd() throws RyaDAOException, MongoException, IOException {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final RyaStatementBuilder builder = new RyaStatementBuilder();
            builder.setPredicate(new RyaURI("http://temp.com"));
            builder.setSubject(new RyaURI("http://subject.com"));
            builder.setObject(new RyaURI("http://object.com"));
            builder.setColumnVisibility(new DocumentVisibility("B").flatten());

            final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

            dao.add(builder.build());

            assertEquals(coll.count(),1);

            final Document dbo = coll.find().first();
            assertTrue(dbo.containsKey(DOCUMENT_VISIBILITY));
            assertTrue(dbo.containsKey(TIMESTAMP));
        }  finally {
            dao.destroy();
        }
    }

    @Test
    public void testDelete() throws RyaDAOException, MongoException, IOException {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final RyaStatementBuilder builder = new RyaStatementBuilder();
            builder.setPredicate(new RyaURI("http://temp.com"));
            builder.setSubject(new RyaURI("http://subject.com"));
            builder.setObject(new RyaURI("http://object.com"));
            builder.setColumnVisibility(new DocumentVisibility("C").flatten());
            final RyaStatement statement = builder.build();
            final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

            dao.add(statement);
            assertEquals(1, coll.count());

            dao.delete(statement, conf);
            assertEquals(0, coll.count());
        } finally {
            dao.destroy();
        }
    }

    @Test
    public void testDeleteWildcardSubjectWithContext() throws RyaDAOException, MongoException, IOException {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final RyaStatementBuilder builder = new RyaStatementBuilder();
            builder.setPredicate(new RyaURI("http://temp.com"));
            builder.setSubject(new RyaURI("http://subject.com"));
            builder.setObject(new RyaURI("http://object.com"));
            builder.setContext(new RyaURI("http://context.com"));
            builder.setColumnVisibility(new DocumentVisibility("A&B&C").flatten());
            final RyaStatement statement = builder.build();

            final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

            dao.add(statement);
            assertEquals(1, coll.count());

            final RyaStatementBuilder builder2 = new RyaStatementBuilder();
            builder2.setPredicate(new RyaURI("http://temp.com"));
            builder2.setObject(new RyaURI("http://object.com"));
            builder2.setContext(new RyaURI("http://context3.com"));
            final RyaStatement query = builder2.build();

            dao.delete(query, conf);
            assertEquals(1, coll.count());
        } finally {
            dao.destroy();
        }
    }

    @Test
    public void testVisibility() throws RyaDAOException, MongoException, IOException {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            // Doc requires "A" and user has "B" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A", new Authorizations("B")));

            // Doc requires "A" and user has "A" = User can view
            assertTrue(testVisibilityStatement(dao, "A", new Authorizations("A")));

            // Doc requires "A" and "B" and user has "A" and "B" = User can view
            assertTrue(testVisibilityStatement(dao, "A&B", new Authorizations("A", "B")));

            // Doc requires "A" or "B" and user has "A" and "B" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B", new Authorizations("A", "B")));

            // Doc requires "A" and user has "A" and "B" = User can view
            assertTrue(testVisibilityStatement(dao, "A", new Authorizations("A", "B")));

            // Doc requires "A" and user has "A" and "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "A", new Authorizations("A", "B", "C")));

            // Doc requires "A" and "B" and user has "A" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A&B", new Authorizations("A")));

            // Doc requires "A" and "B" and user has "B" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A&B", new Authorizations("B")));

            // Doc requires "A" and "B" and user has "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A&B", new Authorizations("C")));

            // Doc requires "A" and "B" and "C" and user has "A" and "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "A&B&C", new Authorizations("A", "B", "C")));

            // Doc requires "A" and "B" and "C" and user has "A" and "B" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A&B&C", new Authorizations("A", "B")));

            // Doc requires "A" and "B" and "C" and user has "A" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A&B&C", new Authorizations("A")));

            // Doc requires "A" and "B" and "C" and user has "B" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A&B&C", new Authorizations("B")));

            // Doc requires "A" and "B" and "C" and user has "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A&B&C", new Authorizations("C")));

            // Doc requires "A" and "B" and user has "A" and "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "A&B", new Authorizations("A", "B", "C")));

            // Doc requires "A" or "B" and user has "A" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B", new Authorizations("A")));

            // Doc requires "A" or "B" and user has "B" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B", new Authorizations("B")));

            // Doc requires "A" or "B" and user has "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|B", new Authorizations("C")));

            // Doc requires "A" or "B" or "C" and user has "A" and "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B|C", new Authorizations("A", "B", "C")));

            // Doc requires "A" or "B" or "C" and user has "A" and "B" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B|C", new Authorizations("A", "B")));

            // Doc requires "A" or "B" or "C" and user has "A" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B|C", new Authorizations("A")));

            // Doc requires "A" or "B" or "C" and user has "B" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B|C", new Authorizations("B")));

            // Doc requires "A" or "B" or "C" and user has "C" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B|C", new Authorizations("C")));

            // Doc requires "A" or "B" and user has "A" and "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B", new Authorizations("A", "B", "C")));

            // Doc requires "A" and user has ALL_AUTHORIZATIONS = User can view
            assertTrue(testVisibilityStatement(dao, "A", MongoDbRdfConstants.ALL_AUTHORIZATIONS));

            // Doc requires "A" and "B" and user has ALL_AUTHORIZATIONS = User can view
            assertTrue(testVisibilityStatement(dao, "A&B", MongoDbRdfConstants.ALL_AUTHORIZATIONS));

            // Doc requires "A" or "B" and user has ALL_AUTHORIZATIONS = User can view
            assertTrue(testVisibilityStatement(dao, "A|B", MongoDbRdfConstants.ALL_AUTHORIZATIONS));

            // Doc has no requirement and user has ALL_AUTHORIZATIONS = User can view
            assertTrue(testVisibilityStatement(dao, "", MongoDbRdfConstants.ALL_AUTHORIZATIONS));

            // Doc has no requirement and user has "A" = User can view
            assertTrue(testVisibilityStatement(dao, "", new Authorizations("A")));

            // Doc has no requirement and user has "A" and "B" = User can view
            assertTrue(testVisibilityStatement(dao, "", new Authorizations("A", "B")));

            // Doc requires "A" or ("B" and "C") and user has "A" = User can view
            assertTrue(testVisibilityStatement(dao, "A|(B&C)", new Authorizations("A")));

            // Doc requires "A" or ("B" and "C") and user has "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "A|(B&C)", new Authorizations("B", "C")));

            // Doc requires "A" or ("B" and "C") and user has "B" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|(B&C)", new Authorizations("B")));

            // Doc requires "A" or ("B" and "C") and user has "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|(B&C)", new Authorizations("C")));

            // Doc requires "A" and ("B" or "C") and user has "A" and "B" = User can view
            assertTrue(testVisibilityStatement(dao, "A&(B|C)", new Authorizations("A", "B")));

            // Doc requires "A" and ("B" or "C") and user has "A" and "B" = User can view
            assertTrue(testVisibilityStatement(dao, "A&(B|C)", new Authorizations("A", "C")));

            // Doc requires "A" and ("B" or "C") and user has "B" and "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A&(B|C)", new Authorizations("B", "C")));

            // Doc requires "A" and ("B" or "C") and user has "A" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A&(B|C)", new Authorizations("A")));

            // Doc requires "A" and ("B" or "C") and user has "B" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A&(B|C)", new Authorizations("B")));

            // Doc requires "A" and ("B" or "C") and user has "C" = User can view
            assertFalse(testVisibilityStatement(dao, "A&(B|C)", new Authorizations("C")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "A" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("A")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "B" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("B")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("C")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "D" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("D")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "E" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("E")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "A" and "B" = User can view
            assertTrue(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("A", "B")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "C" and "D" = User can view
            assertTrue(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("C", "D")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "A" and "B" and "E" = User can view
            assertTrue(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("A", "B", "E")));

            // Doc requires ("A" and "B")mongoClient or ("C" and "D") and user has
            // "C" and "D" and "E" = User can view
            assertTrue(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("C", "D", "E")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "A" and "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("A", "C")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "A" and "D" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("A", "D")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "B" and "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("B", "C")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "B" and "D" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("B", "D")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "A" and "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("A", "B", "C")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "A" and "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("A", "B", "D")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "A" and "C" and "D" = User can view
            assertTrue(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("A", "C", "D")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "B" and "C" and "D" = User can view
            assertTrue(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("B", "C", "D")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "B" and "C" and "D" and "E"= User can view
            assertTrue(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("B", "C", "D", "E")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "A" and "B" and "C" and "D" = User can view
            assertTrue(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("A", "B", "C", "D")));

            // Doc requires ("A" and "B") or ("C" and "D") and user has "A" and "B" and "C" and "D" and "E" = User can view
            assertTrue(testVisibilityStatement(dao, "(A&B)|(C&D)", new Authorizations("A", "B", "C", "D", "E")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "A" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("A")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "B" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("B")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("C")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "D" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("D")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "E" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("E")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "A" and "B" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("A", "B")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "C" and "D" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("C", "D")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "A" and "B" and "E" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("A", "B", "E")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "C" and "D" and "E" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("C", "D", "E")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "A" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("A", "C")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "A" and "D" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("A", "D")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("B", "C")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "B" and "D" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("B", "D")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "B" and "D" and "E" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("B", "D", "E")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "A" and "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("A", "B", "C")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "A" and "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("A", "B", "D")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "A" and "C" and "D" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("A", "C", "D")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "B" and "C" and "D" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("B", "C", "D")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "B" and "C" and "D" and "E"= User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("B", "C", "D", "E")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "A" and "B" and "C" and "D" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("A", "B", "C", "D")));

            // Doc requires ("A" or "B") and ("C" or "D") and user has "A" and "B" and "C" and "D" and "E" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|D)", new Authorizations("A", "B", "C", "D", "E")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "A" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("A", "C")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "B" and "C" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("B", "C")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "A" and "D" and "E" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("A", "D", "E")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "B" and "D" and "E" = User can view
            assertTrue(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("B", "D", "E")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "A" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("A")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "B" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("B")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("C")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "D" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("D")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "E" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("E")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "D" and "E" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("D", "E")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "A" and "D" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("A", "D")));

            // Doc requires "(A|B)&(C|(D&E))" and user has "B" and "E" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "(A|B)&(C|(D&E))", new Authorizations("B", "E")));

            // Doc requires "A|(B&C&(D|E))" and user has "A" = User can view
            assertTrue(testVisibilityStatement(dao, "A|(B&C&(D|E))", new Authorizations("A")));

            // Doc requires "A|(B&C&(D|E))" and user has "B" and "C" and "D" = User can view
            assertTrue(testVisibilityStatement(dao, "A|(B&C&(D|E))", new Authorizations("B", "C", "D")));

            // Doc requires "A|(B&C&(D|E))" and user has "B" and "C" and "E" = User can view
            assertTrue(testVisibilityStatement(dao, "A|(B&C&(D|E))", new Authorizations("B", "C", "E")));

            // Doc requires "A|(B&C&(D|E))" and user has "B" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|(B&C&(D|E))", new Authorizations("B")));

            // Doc requires "A|(B&C&(D|E))" and user has "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|(B&C&(D|E))", new Authorizations("C")));

            // Doc requires "A|(B&C&(D|E))" and user has "D" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|(B&C&(D|E))", new Authorizations("D")));

            // Doc requires "A|(B&C&(D|E))" and user has "E" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|(B&C&(D|E))", new Authorizations("E")));

            // Doc requires "A|(B&C&(D|E))" and user has "B" and "C" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|(B&C&(D|E))", new Authorizations("B", "C")));

            // Doc requires "A|(B&C&(D|E))" and user has "D" and "E" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|(B&C&(D|E))", new Authorizations("D", "E")));

            // Doc requires "A|B|C|D|(E&F&G&H)" and user has "A" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B|C|D|(E&F&G&H)", new Authorizations("A")));

            // Doc requires "A|B|C|D|(E&F&G&H)" and user has "E" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|B|C|D|(E&F&G&H)", new Authorizations("E")));

            // Doc requires "A|B|C|D|(E&F&G&H)" and user has "E" and "F" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|B|C|D|(E&F&G&H)", new Authorizations("E", "F")));

            // Doc requires "A|B|C|D|(E&F&G&H)" and user has "I" = User CANNOT view
            assertFalse(testVisibilityStatement(dao, "A|B|C|D|(E&F&G&H)", new Authorizations("I")));

            // Doc requires "A|B|C|D|(E&F&G&H)" and user has "A" and "I" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B|C|D|(E&F&G&H)", new Authorizations("A", "I")));

            // Doc requires "A|B|C|D|(E&F&G&H)" and user has "E" and "F" and "G" and "H" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B|C|D|(E&F&G&H)", new Authorizations("E", "F", "G", "H")));

            // Doc requires "A|B|C|D|(E&F&G&H)" and user has "E" and "F" and "G" and "H" and "I" = User can view
            assertTrue(testVisibilityStatement(dao, "A|B|C|D|(E&F&G&H)", new Authorizations("E", "F", "G", "H", "I")));

            // Doc has no requirement and user has ALL_AUTHORIZATIONS = User can view
            assertTrue(testVisibilityStatement(dao, null, MongoDbRdfConstants.ALL_AUTHORIZATIONS));

            // Doc has no requirement and user has "A" = User can view
            assertTrue(testVisibilityStatement(dao, null, new Authorizations("A")));

            // Doc has no requirement and user has "A" and "B" = User can view
            assertTrue(testVisibilityStatement(dao, null, new Authorizations("A", "B")));

            // Doc has no requirement and user has no authorizations = User can view
            assertTrue(testVisibilityStatement(dao, null, null));

            // Doc has no requirement and user has no authorizations = User can view
            assertTrue(testVisibilityStatement(dao, "", null));

            // Doc requires "A" and user has no authorizations = User can view
            assertTrue(testVisibilityStatement(dao, "A", null));

            // Doc requires "A" and "B" and user has no authorizations = User can view
            assertTrue(testVisibilityStatement(dao, "A&B", null));

            // Doc requires "A" or "B" and user has no authorizations = User can view
            assertTrue(testVisibilityStatement(dao, "A|B", null));
        } finally {
            dao.destroy();
        }
    }

    /**
     * Generates a test statement with the provided document visibility to
     * determine if the specified user authorization can view the statement.
     * @param documentVisibility the document visibility boolean expression
     * string.
     * @param userAuthorizations the user authorization strings.
     * @return {@code true} if provided authorization could access the document
     * in the collection. {@code false} otherwise.
     * @throws RyaDAOException
     */
    private boolean testVisibilityStatement(
            final MongoDBRyaDAO dao,
            final String documentVisibility,
            final Authorizations userAuthorizations) throws RyaDAOException {
        final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
        final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

        final RyaStatement statement = buildVisibilityTestRyaStatement(documentVisibility);

        dao.getConf().setAuths(AuthorizationsUtil.getAuthorizationsStringArray(Authorizations.EMPTY));
        dao.add(statement);
        dao.getConf().setAuths(AuthorizationsUtil.getAuthorizationsStringArray(userAuthorizations != null ? userAuthorizations : Authorizations.EMPTY));

        assertEquals(1, coll.count());

        final MongoDBQueryEngine queryEngine = (MongoDBQueryEngine) dao.getQueryEngine();
        queryEngine.setConf(conf);
        final CloseableIterable<RyaStatement> iter = queryEngine.query(new RyaQuery(statement));

        // Check if user has authorization to view document based on its visibility
        final boolean hasNext = iter.iterator().hasNext();

        // Reset
        dao.delete(statement, conf);
        assertEquals(0, coll.count());
        dao.getConf().setAuths(AuthorizationsUtil.getAuthorizationsStringArray(Authorizations.EMPTY));

        return hasNext;
    }

    private static RyaStatement buildVisibilityTestRyaStatement(final String documentVisibility) {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setPredicate(new RyaURI("http://temp.com"));
        builder.setSubject(new RyaURI("http://subject.com"));
        builder.setObject(new RyaURI("http://object.com"));
        builder.setContext(new RyaURI("http://context.com"));
        builder.setColumnVisibility(documentVisibility != null ? documentVisibility.getBytes() : null);
        final RyaStatement statement = builder.build();
        return statement;
    }
}
