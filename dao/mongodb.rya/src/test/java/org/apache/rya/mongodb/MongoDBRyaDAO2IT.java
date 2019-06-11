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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.document.visibility.DocumentVisibility;
import org.bson.Document;
import org.junit.Test;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Integration tests the methods of {@link MongoDBRyaDAO}.
 */
public class MongoDBRyaDAO2IT extends MongoRyaITBase {

    @Test
    public void testDeleteWildcard() throws RyaDAOException {
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final RyaStatementBuilder builder = new RyaStatementBuilder();
            builder.setPredicate(new RyaIRI("http://temp.com"));
            dao.delete(builder.build(), conf);
        } finally {
            dao.destroy();
        }
    }


    @Test
    public void testAdd() throws RyaDAOException, MongoException, IOException {
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final RyaStatementBuilder builder = new RyaStatementBuilder();
            builder.setPredicate(new RyaIRI("http://temp.com"));
            builder.setSubject(new RyaIRI("http://subject.com"));
            builder.setObject(new RyaIRI("http://object.com"));

            final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

            dao.add(builder.build());

            assertEquals(coll.countDocuments(), 1);
        }  finally {
            dao.destroy();
        }
    }

    @Test
    public void testDelete() throws RyaDAOException, MongoException, IOException {
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final RyaStatementBuilder builder = new RyaStatementBuilder();
            builder.setPredicate(new RyaIRI("http://temp.com"));
            builder.setSubject(new RyaIRI("http://subject.com"));
            builder.setObject(new RyaIRI("http://object.com"));
            final RyaStatement statement = builder.build();

            final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

            dao.add(statement);
            assertEquals(coll.countDocuments(), 1);

            dao.delete(statement, conf);
            assertEquals(coll.countDocuments(), 0);
        } finally {
            dao.destroy();
        }
    }

    @Test
    public void testDeleteWildcardSubjectWithContext() throws RyaDAOException, MongoException, IOException {
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final RyaStatementBuilder builder = new RyaStatementBuilder();
            builder.setPredicate(new RyaIRI("http://temp.com"));
            builder.setSubject(new RyaIRI("http://subject.com"));
            builder.setObject(new RyaIRI("http://object.com"));
            builder.setContext(new RyaIRI("http://context.com"));
            final RyaStatement statement = builder.build();

            final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

            dao.add(statement);
            assertEquals(coll.countDocuments(), 1);

            final RyaStatementBuilder builder2 = new RyaStatementBuilder();
            builder2.setPredicate(new RyaIRI("http://temp.com"));
            builder2.setObject(new RyaIRI("http://object.com"));
            builder2.setContext(new RyaIRI("http://context3.com"));
            final RyaStatement query = builder2.build();

            dao.delete(query, conf);
            assertEquals(coll.countDocuments(), 1);
        } finally {
            dao.destroy();
        }
    }

    @Test
    public void testReconstructDao() throws RyaDAOException, IOException {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final RyaStatementBuilder builder = new RyaStatementBuilder();
            builder.setPredicate(new RyaIRI("http://temp.com"));
            builder.setSubject(new RyaIRI("http://subject.com"));
            builder.setObject(new RyaIRI("http://object.com"));
            builder.setColumnVisibility(new DocumentVisibility("B").flatten());

            final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

            dao.add(builder.build());

            assertEquals(coll.countDocuments(), 1);

            final Document doc = coll.find().first();
            assertTrue(doc.containsKey(DOCUMENT_VISIBILITY));
            assertTrue(doc.containsKey(TIMESTAMP));
        }  finally {
            dao.destroy();
        }

        // Test reinitializing the same instance
        try {
            dao.init();
        } finally {
            dao.destroy();
        }

        // Reconstruct new DAO and try again
        dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final RyaStatementBuilder builder = new RyaStatementBuilder();
            builder.setPredicate(new RyaIRI("http://temp.com"));
            builder.setSubject(new RyaIRI("http://subject.com"));
            builder.setObject(new RyaIRI("http://object.com"));
            builder.setColumnVisibility(new DocumentVisibility("B").flatten());

            final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

            dao.add(builder.build());

            assertEquals(coll.countDocuments(), 1);

            final Document doc = coll.find().first();
            assertTrue(doc.containsKey(DOCUMENT_VISIBILITY));
            assertTrue(doc.containsKey(TIMESTAMP));
        }  finally {
            dao.destroy();
        }
    }
}