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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.bson.Document;
import org.junit.Test;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Integration tests the methods of {@link MongoDBRyaDAO}.
 */
public class MongoDBRyaDAO2IT extends MongoITBase {

    @Test
    public void testDeleteWildcard() throws RyaDAOException {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final RyaStatementBuilder builder = new RyaStatementBuilder();
            builder.setPredicate(new RyaURI("http://temp.com"));
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

            final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

            dao.add(builder.build());

            assertEquals(coll.count(),1);
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
            final RyaStatement statement = builder.build();

            final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

            dao.add(statement);
            assertEquals(coll.count(),1);

            dao.delete(statement, conf);
            assertEquals(coll.count(),0);
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
            final RyaStatement statement = builder.build();

            final MongoDatabase db = conf.getMongoClient().getDatabase(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            final MongoCollection<Document> coll = db.getCollection(conf.getTriplesCollectionName());

            dao.add(statement);
            assertEquals(coll.count(),1);

            final RyaStatementBuilder builder2 = new RyaStatementBuilder();
            builder2.setPredicate(new RyaURI("http://temp.com"));
            builder2.setObject(new RyaURI("http://object.com"));
            builder2.setContext(new RyaURI("http://context3.com"));
            final RyaStatement query = builder2.build();

            dao.delete(query, conf);
            assertEquals(coll.count(),1);
        } finally {
            dao.destroy();
        }
    }
}