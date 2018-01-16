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
package org.apache.rya.sail.config;

import static org.junit.Assert.assertFalse;

import org.apache.rya.mongodb.MongoITBase;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

/**
 * Tests {@link RyaSailFactory} with a MongoDB backend.
 */
public class RyaMongoDbSailFactoryTest extends MongoITBase {
    @Test
    public void testCreateMongoDbSail() throws Exception {
        Sail sail = null;
        SailRepository repo = null;
        SailRepositoryConnection conn = null;
        try {
            sail = RyaSailFactory.getInstance(conf);
            repo = new SailRepository(sail);
            conn = repo.getConnection();
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (repo != null) {
                repo.shutDown();
            }
            if (sail != null) {
                sail.shutDown();
            }
        }
    }

    @Test
    public void testAddStatement() throws Exception {
        Sail sail = null;
        SailRepository repo = null;
        SailRepositoryConnection conn = null;
        try {
            sail = RyaSailFactory.getInstance(conf);
            repo = new SailRepository(sail);
            conn = repo.getConnection();

            final ValueFactory vf = conn.getValueFactory();
            final Statement s = vf.createStatement(vf.createURI("u:a"), vf.createURI("u:b"), vf.createURI("u:c"));

            assertFalse(conn.hasStatement(s, false));

            conn.add(s);

            Assert.assertTrue(conn.hasStatement(s, false));
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (repo != null) {
                repo.shutDown();
            }
            if (sail != null) {
                sail.shutDown();
            }
        }
    }

    @Test
    public void testReuseSail() throws Exception {
        Sail sail = null;
        SailRepository repo = null;
        SailRepositoryConnection conn = null;
        try {
            sail = RyaSailFactory.getInstance(conf);
            repo = new SailRepository(sail);
            conn = repo.getConnection();

            final ValueFactory vf = conn.getValueFactory();
            final Statement s = vf.createStatement(vf.createURI("u:a"), vf.createURI("u:b"), vf.createURI("u:c"));

            assertFalse(conn.hasStatement(s, false));

            conn.add(s);

            Assert.assertTrue(conn.hasStatement(s, false));

            conn.remove(s);

            Assert.assertFalse(conn.hasStatement(s, false));
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (repo != null) {
                repo.shutDown();
            }
            if (sail != null) {
                sail.shutDown();
            }
        }

        // Reuse Sail after shutdown
        try {
            sail = RyaSailFactory.getInstance(conf);
            repo = new SailRepository(sail);
            conn = repo.getConnection();

            final ValueFactory vf = conn.getValueFactory();
            final Statement s = vf.createStatement(vf.createURI("u:a"), vf.createURI("u:b"), vf.createURI("u:c"));

            assertFalse(conn.hasStatement(s, false));

            conn.add(s);

            Assert.assertTrue(conn.hasStatement(s, false));
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (repo != null) {
                repo.shutDown();
            }
            if (sail != null) {
                sail.shutDown();
            }
        }
    }
}