package org.apache.rya;

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



import info.aduna.iteration.CloseableIteration;
import junit.framework.TestCase;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.join.HashJoin;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * Date: 7/24/12
 * Time: 5:51 PM
 */
public class HashJoinTest {
    private AccumuloRyaDAO dao;
    static String litdupsNS = "urn:test:litdups#";
    private Connector connector;
    private AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

    @Before
    public void init() throws Exception {
        dao = new AccumuloRyaDAO();
        connector = new MockInstance().getConnector("", "");
        dao.setConnector(connector);
        dao.setConf(conf);
        dao.init();
    }

    @After
    public void destroy() throws Exception {
        dao.destroy();
    }

    @Test
    public void testSimpleJoin() throws Exception {
        //add data
        RyaURI pred = new RyaURI(litdupsNS, "pred1");
        RyaType one = new RyaType("1");
        RyaType two = new RyaType("2");
        RyaURI subj1 = new RyaURI(litdupsNS, "subj1");
        RyaURI subj2 = new RyaURI(litdupsNS, "subj2");
        RyaURI subj3 = new RyaURI(litdupsNS, "subj3");
        RyaURI subj4 = new RyaURI(litdupsNS, "subj4");

        dao.add(new RyaStatement(subj1, pred, one));
        dao.add(new RyaStatement(subj1, pred, two));
        dao.add(new RyaStatement(subj2, pred, one));
        dao.add(new RyaStatement(subj2, pred, two));
        dao.add(new RyaStatement(subj3, pred, one));
        dao.add(new RyaStatement(subj3, pred, two));
        dao.add(new RyaStatement(subj4, pred, one));
        dao.add(new RyaStatement(subj4, pred, two));
        

        //1 join
        HashJoin hjoin = new HashJoin(dao.getQueryEngine());
        CloseableIteration<RyaURI, RyaDAOException> join = hjoin.join(null,
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, one),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, two));

        Set<RyaURI> uris = new HashSet<RyaURI>();
        while (join.hasNext()) {
            uris.add(join.next());
        }
        assertTrue(uris.contains(subj1));
        assertTrue(uris.contains(subj2));
        assertTrue(uris.contains(subj3));
        assertTrue(uris.contains(subj4));
        join.close();
    }

    @Test
    public void testSimpleJoinMultiWay() throws Exception {
        //add data
        RyaURI pred = new RyaURI(litdupsNS, "pred1");
        RyaType one = new RyaType("1");
        RyaType two = new RyaType("2");
        RyaType three = new RyaType("3");
        RyaType four = new RyaType("4");
        RyaURI subj1 = new RyaURI(litdupsNS, "subj1");
        RyaURI subj2 = new RyaURI(litdupsNS, "subj2");
        RyaURI subj3 = new RyaURI(litdupsNS, "subj3");
        RyaURI subj4 = new RyaURI(litdupsNS, "subj4");

        dao.add(new RyaStatement(subj1, pred, one));
        dao.add(new RyaStatement(subj1, pred, two));
        dao.add(new RyaStatement(subj1, pred, three));
        dao.add(new RyaStatement(subj1, pred, four));
        dao.add(new RyaStatement(subj2, pred, one));
        dao.add(new RyaStatement(subj2, pred, two));
        dao.add(new RyaStatement(subj2, pred, three));
        dao.add(new RyaStatement(subj2, pred, four));
        dao.add(new RyaStatement(subj3, pred, one));
        dao.add(new RyaStatement(subj3, pred, two));
        dao.add(new RyaStatement(subj3, pred, three));
        dao.add(new RyaStatement(subj3, pred, four));
        dao.add(new RyaStatement(subj4, pred, one));
        dao.add(new RyaStatement(subj4, pred, two));
        dao.add(new RyaStatement(subj4, pred, three));
        dao.add(new RyaStatement(subj4, pred, four));
        

        //1 join
        HashJoin hjoin = new HashJoin(dao.getQueryEngine());
        CloseableIteration<RyaURI, RyaDAOException> join = hjoin.join(null,
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, one),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, two),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, three),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, four)
        );

        Set<RyaURI> uris = new HashSet<RyaURI>();
        while (join.hasNext()) {
            uris.add(join.next());
        }
        assertTrue(uris.contains(subj1));
        assertTrue(uris.contains(subj2));
        assertTrue(uris.contains(subj3));
        assertTrue(uris.contains(subj4));
        join.close();
    }

    @Test
    public void testMergeJoinMultiWay() throws Exception {
        //add data
        RyaURI pred = new RyaURI(litdupsNS, "pred1");
        RyaType zero = new RyaType("0");
        RyaType one = new RyaType("1");
        RyaType two = new RyaType("2");
        RyaType three = new RyaType("3");
        RyaType four = new RyaType("4");
        RyaURI subj1 = new RyaURI(litdupsNS, "subj1");
        RyaURI subj2 = new RyaURI(litdupsNS, "subj2");
        RyaURI subj3 = new RyaURI(litdupsNS, "subj3");
        RyaURI subj4 = new RyaURI(litdupsNS, "subj4");

        dao.add(new RyaStatement(subj1, pred, one));
        dao.add(new RyaStatement(subj1, pred, two));
        dao.add(new RyaStatement(subj1, pred, three));
        dao.add(new RyaStatement(subj1, pred, four));
        dao.add(new RyaStatement(subj2, pred, zero));
        dao.add(new RyaStatement(subj2, pred, one));
        dao.add(new RyaStatement(subj2, pred, two));
        dao.add(new RyaStatement(subj2, pred, three));
        dao.add(new RyaStatement(subj2, pred, four));
        dao.add(new RyaStatement(subj3, pred, one));
        dao.add(new RyaStatement(subj3, pred, two));
        dao.add(new RyaStatement(subj3, pred, four));
        dao.add(new RyaStatement(subj4, pred, one));
        dao.add(new RyaStatement(subj4, pred, two));
        dao.add(new RyaStatement(subj4, pred, three));
        dao.add(new RyaStatement(subj4, pred, four));
        

        //1 join
        HashJoin hjoin = new HashJoin(dao.getQueryEngine());
        CloseableIteration<RyaURI, RyaDAOException> join = hjoin.join(null,
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, one),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, two),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, three),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, four)
        );

        Set<RyaURI> uris = new HashSet<RyaURI>();
        while (join.hasNext()) {
            uris.add(join.next());
        }
        assertTrue(uris.contains(subj1));
        assertTrue(uris.contains(subj2));
        assertTrue(uris.contains(subj4));
        join.close();
    }

    @Test
    public void testMergeJoinMultiWayNone() throws Exception {
        //add data
        RyaURI pred = new RyaURI(litdupsNS, "pred1");
        RyaType zero = new RyaType("0");
        RyaType one = new RyaType("1");
        RyaType two = new RyaType("2");
        RyaType three = new RyaType("3");
        RyaType four = new RyaType("4");
        RyaURI subj1 = new RyaURI(litdupsNS, "subj1");
        RyaURI subj2 = new RyaURI(litdupsNS, "subj2");
        RyaURI subj3 = new RyaURI(litdupsNS, "subj3");
        RyaURI subj4 = new RyaURI(litdupsNS, "subj4");

        dao.add(new RyaStatement(subj1, pred, one));
        dao.add(new RyaStatement(subj1, pred, three));
        dao.add(new RyaStatement(subj1, pred, four));
        dao.add(new RyaStatement(subj2, pred, zero));
        dao.add(new RyaStatement(subj2, pred, one));
        dao.add(new RyaStatement(subj2, pred, four));
        dao.add(new RyaStatement(subj3, pred, two));
        dao.add(new RyaStatement(subj3, pred, four));
        dao.add(new RyaStatement(subj4, pred, one));
        dao.add(new RyaStatement(subj4, pred, two));
        dao.add(new RyaStatement(subj4, pred, three));
        

        //1 join
        HashJoin hjoin = new HashJoin(dao.getQueryEngine());
        CloseableIteration<RyaURI, RyaDAOException> join = hjoin.join(null,
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, one),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, two),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, three),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, four)
        );

        assertFalse(join.hasNext());
        join.close();
    }

    @Test
    public void testMergeJoinMultiWayNone2() throws Exception {
        //add data
        RyaURI pred = new RyaURI(litdupsNS, "pred1");
        RyaType zero = new RyaType("0");
        RyaType one = new RyaType("1");
        RyaType two = new RyaType("2");
        RyaType three = new RyaType("3");
        RyaType four = new RyaType("4");
        RyaURI subj1 = new RyaURI(litdupsNS, "subj1");
        RyaURI subj2 = new RyaURI(litdupsNS, "subj2");
        RyaURI subj3 = new RyaURI(litdupsNS, "subj3");
        RyaURI subj4 = new RyaURI(litdupsNS, "subj4");

        dao.add(new RyaStatement(subj1, pred, one));
        dao.add(new RyaStatement(subj1, pred, four));
        dao.add(new RyaStatement(subj2, pred, zero));
        dao.add(new RyaStatement(subj2, pred, one));
        dao.add(new RyaStatement(subj2, pred, four));
        dao.add(new RyaStatement(subj3, pred, two));
        dao.add(new RyaStatement(subj3, pred, four));
        dao.add(new RyaStatement(subj4, pred, one));
        dao.add(new RyaStatement(subj4, pred, two));
        

        //1 join
        HashJoin hjoin = new HashJoin(dao.getQueryEngine());
        CloseableIteration<RyaURI, RyaDAOException> join = hjoin.join(null,
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, one),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, two),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, three),
                new RdfCloudTripleStoreUtils.CustomEntry<RyaURI, RyaType>(pred, four)
        );

        assertFalse(join.hasNext());
        join.close();
    }

    @Test
    public void testSimpleHashJoinPredicateOnly() throws Exception {
        //add data
        RyaURI pred1 = new RyaURI(litdupsNS, "pred1");
        RyaURI pred2 = new RyaURI(litdupsNS, "pred2");
        RyaType one = new RyaType("1");
        RyaURI subj1 = new RyaURI(litdupsNS, "subj1");
        RyaURI subj2 = new RyaURI(litdupsNS, "subj2");
        RyaURI subj3 = new RyaURI(litdupsNS, "subj3");
        RyaURI subj4 = new RyaURI(litdupsNS, "subj4");

        dao.add(new RyaStatement(subj1, pred1, one));
        dao.add(new RyaStatement(subj1, pred2, one));
        dao.add(new RyaStatement(subj2, pred1, one));
        dao.add(new RyaStatement(subj2, pred2, one));
        dao.add(new RyaStatement(subj3, pred1, one));
        dao.add(new RyaStatement(subj3, pred2, one));
        dao.add(new RyaStatement(subj4, pred1, one));
        dao.add(new RyaStatement(subj4, pred2, one));
        

        //1 join
        HashJoin ijoin = new HashJoin(dao.getQueryEngine());
        CloseableIteration<RyaStatement, RyaDAOException> join = ijoin.join(null, pred1, pred2);

        int count = 0;
        while (join.hasNext()) {
            RyaStatement next = join.next();
            count++;
        }
        assertEquals(4, count);
        join.close();
    }

    @Test
    public void testSimpleMergeJoinPredicateOnly2() throws Exception {
        //add data
        RyaURI pred1 = new RyaURI(litdupsNS, "pred1");
        RyaURI pred2 = new RyaURI(litdupsNS, "pred2");
        RyaType one = new RyaType("1");
        RyaType two = new RyaType("2");
        RyaType three = new RyaType("3");
        RyaURI subj1 = new RyaURI(litdupsNS, "subj1");
        RyaURI subj2 = new RyaURI(litdupsNS, "subj2");
        RyaURI subj3 = new RyaURI(litdupsNS, "subj3");
        RyaURI subj4 = new RyaURI(litdupsNS, "subj4");

        dao.add(new RyaStatement(subj1, pred1, one));
        dao.add(new RyaStatement(subj1, pred1, two));
        dao.add(new RyaStatement(subj1, pred1, three));
        dao.add(new RyaStatement(subj1, pred2, one));
        dao.add(new RyaStatement(subj1, pred2, two));
        dao.add(new RyaStatement(subj1, pred2, three));
        dao.add(new RyaStatement(subj2, pred1, one));
        dao.add(new RyaStatement(subj2, pred1, two));
        dao.add(new RyaStatement(subj2, pred1, three));
        dao.add(new RyaStatement(subj2, pred2, one));
        dao.add(new RyaStatement(subj2, pred2, two));
        dao.add(new RyaStatement(subj2, pred2, three));
        dao.add(new RyaStatement(subj3, pred1, one));
        dao.add(new RyaStatement(subj3, pred1, two));
        dao.add(new RyaStatement(subj3, pred1, three));
        dao.add(new RyaStatement(subj3, pred2, one));
        dao.add(new RyaStatement(subj3, pred2, two));
        dao.add(new RyaStatement(subj3, pred2, three));
        dao.add(new RyaStatement(subj4, pred1, one));
        dao.add(new RyaStatement(subj4, pred1, two));
        dao.add(new RyaStatement(subj4, pred1, three));
        dao.add(new RyaStatement(subj4, pred2, one));
        dao.add(new RyaStatement(subj4, pred2, two));
        dao.add(new RyaStatement(subj4, pred2, three));
        

        //1 join
        HashJoin ijoin = new HashJoin(dao.getQueryEngine());
        CloseableIteration<RyaStatement, RyaDAOException> join = ijoin.join(null, pred1, pred2);

        int count = 0;
        while (join.hasNext()) {
            RyaStatement next = join.next();
            count++;
        }
        assertEquals(12, count);
        join.close();
    }
}
