package org.apache.rya.accumulo.mr;

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

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.client.Connector;
import org.calrissian.mango.collect.CloseableIterable;
import org.junit.Assert;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.query.AccumuloRyaQueryEngine;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQuery;

public class TestUtils {
    public static void verify(Connector connector, AccumuloRdfConfiguration conf, RyaStatement... ryaStatements)
            throws RyaDAOException, IOException {
        AccumuloRyaDAO dao = new AccumuloRyaDAO();
        dao.setConnector(connector);
        dao.setConf(conf);
        dao.init();
        AccumuloRyaQueryEngine engine = dao.getQueryEngine();
        for (RyaStatement ryaStatement : ryaStatements) {
            verify(ryaStatement, engine);
        }
        dao.destroy();
    }

    public static RyaStatement verify(RyaStatement ryaStatement, AccumuloRyaQueryEngine queryEngine)
      throws RyaDAOException, IOException {
        //check osp
        CloseableIterable<RyaStatement> statements =
          queryEngine.query(RyaQuery.builder(new RyaStatement(null, null, ryaStatement.getObject()))
                                    .build());
        try {
            verifyFirstStatement(ryaStatement, statements);
        } finally {
            statements.close();
        }

        //check po
        statements = queryEngine.query(RyaQuery.builder(
          new RyaStatement(null, ryaStatement.getPredicate(),
                           ryaStatement.getObject())).build());
        try {
            verifyFirstStatement(ryaStatement, statements);
        } finally {
            statements.close();
        }

        //check spo
        RyaStatement result;
        statements = queryEngine.query(RyaQuery.builder(
          new RyaStatement(ryaStatement.getSubject(),
                           ryaStatement.getPredicate(),
                           ryaStatement.getObject())).build());
        try {
            result = verifyFirstStatement(ryaStatement, statements);
        } finally {
            statements.close();
        }
        return result;
    }

    private static RyaStatement verifyFirstStatement(
      RyaStatement ryaStatement, CloseableIterable<RyaStatement> statements) {
        final Iterator<RyaStatement> iterator = statements.iterator();
        Assert.assertTrue(iterator.hasNext());
        final RyaStatement first = iterator.next();
        Assert.assertEquals(ryaStatement.getSubject(), first.getSubject());
        Assert.assertEquals(ryaStatement.getPredicate(), first.getPredicate());
        Assert.assertEquals(ryaStatement.getObject(), first.getObject());
        Assert.assertEquals(ryaStatement.getContext(), first.getContext());
        Assert.assertEquals(ryaStatement.getQualifer(), first.getQualifer());
        // Test for equality if provided, otherwise test that these are empty
        if (ryaStatement.getColumnVisibility() == null) {
            Assert.assertEquals("Expected empty visibility.", 0, first.getColumnVisibility().length);
        }
        else {
            Assert.assertArrayEquals("Mismatched visibilities.",
                    ryaStatement.getColumnVisibility(), first.getColumnVisibility());
        }
        if (ryaStatement.getValue() == null) {
            Assert.assertEquals("Expected empty value array.", 0, first.getValue().length);
        }
        else {
            Assert.assertArrayEquals("Mismatched values.", ryaStatement.getValue(), first.getValue());
        }
        Assert.assertFalse(iterator.hasNext());
        return first;
    }
}