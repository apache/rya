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

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaResource;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.join.IterativeJoin;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Date: 7/24/12
 * Time: 5:51 PM
 */
public class IterativeJoinTest {
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
    public void testSimpleIterativeJoin() throws Exception {
        //add data
        RyaIRI pred = new RyaIRI(litdupsNS, "pred1");
        RyaValue one = new RyaType("1");
        RyaValue two = new RyaType("2");
        RyaResource subj1 = new RyaIRI(litdupsNS, "subj1");
        RyaResource subj2 = new RyaIRI(litdupsNS, "subj2");
        RyaResource subj3 = new RyaIRI(litdupsNS, "subj3");
        RyaResource subj4 = new RyaIRI(litdupsNS, "subj4");

        dao.add(new RyaStatement(subj1, pred, one));
        dao.add(new RyaStatement(subj1, pred, two));
        dao.add(new RyaStatement(subj2, pred, one));
        dao.add(new RyaStatement(subj2, pred, two));
        dao.add(new RyaStatement(subj3, pred, one));
        dao.add(new RyaStatement(subj3, pred, two));
        dao.add(new RyaStatement(subj4, pred, one));
        dao.add(new RyaStatement(subj4, pred, two));

        //1 join
        IterativeJoin<AccumuloRdfConfiguration> iterJoin = new IterativeJoin<>(dao.getQueryEngine());
        CloseableIteration<RyaResource, RyaDAOException> join = iterJoin.join(conf,
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, one),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, two));

        Set<RyaResource> uris = new HashSet<>();
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
    public void testSimpleIterativeJoinMultiWay() throws Exception {
        //add data
        RyaIRI pred = new RyaIRI(litdupsNS, "pred1");
        RyaValue one = new RyaType("1");
        RyaValue two = new RyaType("2");
        RyaValue three = new RyaType("3");
        RyaValue four = new RyaType("4");
        RyaResource subj1 = new RyaIRI(litdupsNS, "subj1");
        RyaResource subj2 = new RyaIRI(litdupsNS, "subj2");
        RyaResource subj3 = new RyaIRI(litdupsNS, "subj3");
        RyaResource subj4 = new RyaIRI(litdupsNS, "subj4");

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
        IterativeJoin<AccumuloRdfConfiguration> iterJoin = new IterativeJoin<>(dao.getQueryEngine());
        CloseableIteration<RyaResource, RyaDAOException> join = iterJoin.join(conf,
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, one),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, two),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, three),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, four)
        );

        Set<RyaResource> uris = new HashSet<>();
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
    public void testIterativeJoinMultiWay() throws Exception {
        //add data
        RyaIRI pred = new RyaIRI(litdupsNS, "pred1");
        RyaValue zero = new RyaType("0");
        RyaValue one = new RyaType("1");
        RyaValue two = new RyaType("2");
        RyaValue three = new RyaType("3");
        RyaValue four = new RyaType("4");
        RyaResource subj1 = new RyaIRI(litdupsNS, "subj1");
        RyaResource subj2 = new RyaIRI(litdupsNS, "subj2");
        RyaResource subj3 = new RyaIRI(litdupsNS, "subj3");
        RyaResource subj4 = new RyaIRI(litdupsNS, "subj4");

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
        IterativeJoin<AccumuloRdfConfiguration> iterJoin = new IterativeJoin<>(dao.getQueryEngine());
        CloseableIteration<RyaResource, RyaDAOException> join = iterJoin.join(conf,
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, one),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, two),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, three),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, four)
        );

        Set<RyaResource> uris = new HashSet<>();
        while (join.hasNext()) {
            uris.add(join.next());
        }
        assertTrue(uris.contains(subj1));
        assertTrue(uris.contains(subj2));
        assertTrue(uris.contains(subj4));
        join.close();
    }

    @Test
    public void testIterativeJoinMultiWayNone() throws Exception {
        //add data
        RyaIRI pred = new RyaIRI(litdupsNS, "pred1");
        RyaValue zero = new RyaType("0");
        RyaValue one = new RyaType("1");
        RyaValue two = new RyaType("2");
        RyaValue three = new RyaType("3");
        RyaValue four = new RyaType("4");
        RyaResource subj1 = new RyaIRI(litdupsNS, "subj1");
        RyaResource subj2 = new RyaIRI(litdupsNS, "subj2");
        RyaResource subj3 = new RyaIRI(litdupsNS, "subj3");
        RyaResource subj4 = new RyaIRI(litdupsNS, "subj4");

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
        IterativeJoin<AccumuloRdfConfiguration> iterJoin = new IterativeJoin<>(dao.getQueryEngine());
        CloseableIteration<RyaResource, RyaDAOException> join = iterJoin.join(conf,
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, one),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, two),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, three),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, four)
        );

        assertFalse(join.hasNext());
        join.close();
    }

    @Test
    public void testIterativeJoinMultiWayNone2() throws Exception {
        //add data
        RyaIRI pred = new RyaIRI(litdupsNS, "pred1");
        RyaValue zero = new RyaType("0");
        RyaValue one = new RyaType("1");
        RyaValue two = new RyaType("2");
        RyaValue three = new RyaType("3");
        RyaValue four = new RyaType("4");
        RyaResource subj1 = new RyaIRI(litdupsNS, "subj1");
        RyaResource subj2 = new RyaIRI(litdupsNS, "subj2");
        RyaResource subj3 = new RyaIRI(litdupsNS, "subj3");
        RyaResource subj4 = new RyaIRI(litdupsNS, "subj4");

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
        IterativeJoin<AccumuloRdfConfiguration> iterJoin = new IterativeJoin<>(dao.getQueryEngine());
        CloseableIteration<RyaResource, RyaDAOException> join = iterJoin.join(conf,
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, two),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, three),
                new RdfCloudTripleStoreUtils.CustomEntry<>(pred, four)
        );

        assertFalse(join.hasNext());
        join.close();
    }

    @Test
    public void testSimpleIterativeJoinPredicateOnly() throws Exception {
        //add data
        RyaIRI pred1 = new RyaIRI(litdupsNS, "pred1");
        RyaIRI pred2 = new RyaIRI(litdupsNS, "pred2");
        RyaValue one = new RyaType("1");
        RyaResource subj1 = new RyaIRI(litdupsNS, "subj1");
        RyaResource subj2 = new RyaIRI(litdupsNS, "subj2");
        RyaResource subj3 = new RyaIRI(litdupsNS, "subj3");
        RyaResource subj4 = new RyaIRI(litdupsNS, "subj4");

        dao.add(new RyaStatement(subj1, pred1, one));
        dao.add(new RyaStatement(subj1, pred2, one));
        dao.add(new RyaStatement(subj2, pred1, one));
        dao.add(new RyaStatement(subj2, pred2, one));
        dao.add(new RyaStatement(subj3, pred1, one));
        dao.add(new RyaStatement(subj3, pred2, one));
        dao.add(new RyaStatement(subj4, pred1, one));
        dao.add(new RyaStatement(subj4, pred2, one));
        

        //1 join
        IterativeJoin<AccumuloRdfConfiguration> ijoin = new IterativeJoin<>(dao.getQueryEngine());
        CloseableIteration<RyaStatement, RyaDAOException> join = ijoin.join(conf, pred1, pred2);

        int count = 0;
        while (join.hasNext()) {
            RyaStatement next = join.next();
            count++;
        }
        assertEquals(4, count);
        join.close();
    }

    @Test
    public void testSimpleIterativeJoinPredicateOnly2() throws Exception {
        //add data
        RyaIRI pred1 = new RyaIRI(litdupsNS, "pred1");
        RyaIRI pred2 = new RyaIRI(litdupsNS, "pred2");
        RyaValue one = new RyaType("1");
        RyaValue two = new RyaType("2");
        RyaValue three = new RyaType("3");
        RyaResource subj1 = new RyaIRI(litdupsNS, "subj1");
        RyaResource subj2 = new RyaIRI(litdupsNS, "subj2");
        RyaResource subj3 = new RyaIRI(litdupsNS, "subj3");
        RyaResource subj4 = new RyaIRI(litdupsNS, "subj4");

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
        IterativeJoin<AccumuloRdfConfiguration> ijoin = new IterativeJoin<>(dao.getQueryEngine());
        CloseableIteration<RyaStatement, RyaDAOException> join = ijoin.join(conf, pred1, pred2);

        int count = 0;
        while (join.hasNext()) {
            RyaStatement next = join.next();
            count++;
        }
        assertEquals(12, count);
        join.close();
    }
}
