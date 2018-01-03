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
package org.apache.rya.indexing.mongo;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.GeoRyaSailFactory;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.OptionalConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoITBase;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

public class MongoGeoIndexerFilterIT extends MongoITBase {
    private static final GeometryFactory GF = new GeometryFactory();
    private static final Geometry WASHINGTON_MONUMENT = GF.createPoint(new Coordinate(38.8895, 77.0353));
    private static final Geometry LINCOLN_MEMORIAL = GF.createPoint(new Coordinate(38.8893, 77.0502));
    private static final Geometry CAPITAL_BUILDING = GF.createPoint(new Coordinate(38.8899, 77.0091));
    private static final Geometry WHITE_HOUSE = GF.createPoint(new Coordinate(38.8977, 77.0365));

    @Override
    public void updateConfiguration(final MongoDBRdfConfiguration conf) {
        conf.setBoolean(OptionalConfigUtils.USE_GEO, true);
        conf.set(ConfigUtils.GEO_PREDICATES_LIST, "http://www.opengis.net/ont/geosparql#asWKT");
        conf.setBoolean(ConfigUtils.USE_MONGO, true);
    }

    @Test
    public void nearHappyUsesTest() throws Exception {
        final Sail sail = GeoRyaSailFactory.getInstance(conf);
        final SailRepositoryConnection conn = new SailRepository(sail).getConnection();
        try {
            populateRya(conn);

            //Only captial
            String query =
                    "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n"
                            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n"
                            + "SELECT * \n" //
                            + "WHERE { \n"
                            + "  <urn:geo> geo:asWKT ?point .\n"
                            + "  FILTER(geof:sfNear(?point, \"POINT(38.8895 77.0353)\"^^geo:wktLiteral, 0.0, 2000))"
                            + "}";

            TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
            final List<BindingSet> results = new ArrayList<>();
            while (rez.hasNext()) {
                final BindingSet bs = rez.next();
                results.add(bs);
            }
            assertEquals(1, results.size());
            assertEquals(CAPITAL_BUILDING, bindingToGeo(results.get(0)));

            //all but capital
            query =
                    "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n"
                            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n"
                            + "SELECT * \n" //
                            + "WHERE { \n"
                            + "  <urn:geo> geo:asWKT ?point .\n"
                            + "  FILTER(geof:sfNear(?point, \"POINT(38.8895 77.0353)\"^^geo:wktLiteral, 2000))"
                            + "}";

            rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
            results.clear();
            while (rez.hasNext()) {
                final BindingSet bs = rez.next();
                results.add(bs);
            }
            assertEquals(3, results.size());
            assertEquals(WASHINGTON_MONUMENT, bindingToGeo(results.get(0)));
            assertEquals(WHITE_HOUSE, bindingToGeo(results.get(1)));
            assertEquals(LINCOLN_MEMORIAL, bindingToGeo(results.get(2)));

            // all of them
            query =
                    "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n"
                            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n"
                            + "SELECT * \n" //
                            + "WHERE { \n"
                            + "  <urn:geo> geo:asWKT ?point .\n"
                            + "  FILTER(geof:sfNear(?point, \"POINT(38.8895 77.0353)\"^^geo:wktLiteral, 6000, 000))"
                            + "}";

            rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
            results.clear();
            while (rez.hasNext()) {
                final BindingSet bs = rez.next();
                results.add(bs);
            }
            assertEquals(4, results.size());
            assertEquals(WASHINGTON_MONUMENT, bindingToGeo(results.get(0)));
            assertEquals(WHITE_HOUSE, bindingToGeo(results.get(1)));
            assertEquals(LINCOLN_MEMORIAL, bindingToGeo(results.get(2)));
            assertEquals(CAPITAL_BUILDING, bindingToGeo(results.get(3)));

            // donut, only 2
            query =
                    "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n"
                            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n"
                            + "SELECT * \n" //
                            + "WHERE { \n"
                            + "  <urn:geo> geo:asWKT ?point .\n"
                            + "  FILTER(geof:sfNear(?point, \"POINT(38.8895 77.0353)\"^^geo:wktLiteral, 2000, 100))"
                            + "}";

            rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
            results.clear();
            while (rez.hasNext()) {
                final BindingSet bs = rez.next();
                results.add(bs);
            }
            assertEquals(2, results.size());
            assertEquals(WHITE_HOUSE, bindingToGeo(results.get(0)));
            assertEquals(LINCOLN_MEMORIAL, bindingToGeo(results.get(1)));

            // all of them
            query =
                    "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n"
                            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n"
                            + "SELECT * \n" //
                            + "WHERE { \n"
                            + "  <urn:geo> geo:asWKT ?point .\n"
                            + "  FILTER(geof:sfNear(?point, \"POINT(38.8895 77.0353)\"^^geo:wktLiteral))"
                            + "}";
            rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
            results.clear();
            while (rez.hasNext()) {
                final BindingSet bs = rez.next();
                results.add(bs);
            }
            assertEquals(4, results.size());
            assertEquals(WASHINGTON_MONUMENT, bindingToGeo(results.get(0)));
            assertEquals(WHITE_HOUSE, bindingToGeo(results.get(1)));
            assertEquals(LINCOLN_MEMORIAL, bindingToGeo(results.get(2)));
            assertEquals(CAPITAL_BUILDING, bindingToGeo(results.get(3)));
        } finally {
            conn.close();
            sail.shutDown();
        }
    }

    @Test(expected = MalformedQueryException.class)
    public void near_invalidDistance() throws Exception {
        final Sail sail = GeoRyaSailFactory.getInstance(conf);
        final SailRepositoryConnection conn = new SailRepository(sail).getConnection();
        try {
            populateRya(conn);

            //Only captial
            final String query =
                    "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n"
                            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n"
                            + "SELECT * \n" //
                            + "WHERE { \n"
                            + "  <urn:geo> geo:asWKT ?point .\n"
                            + "  FILTER(geof:sfNear(?point, \"POINT(38.8895 77.0353)\"^^geo:wktLiteral, distance))"
                            + "}";

            conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
        } finally {
            conn.close();
            sail.shutDown();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void near_negativeDistance() throws Exception {
        final Sail sail = GeoRyaSailFactory.getInstance(conf);
        final SailRepositoryConnection conn = new SailRepository(sail).getConnection();
        try {
            populateRya(conn);

            //Only captial
            final String query =
                    "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n"
                            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n"
                            + "SELECT * \n" //
                            + "WHERE { \n"
                            + "  <urn:geo> geo:asWKT ?point .\n"
                            + "  FILTER(geof:sfNear(?point, \"POINT(38.8895 77.0353)\"^^geo:wktLiteral, -100))"
                            + "}";

            final TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
            while(rez.hasNext()) {
                rez.next();
            }
        } finally {
            conn.close();
            sail.shutDown();
        }
    }

    @Test(expected = QueryEvaluationException.class)
    public void tooManyArgumentsTest() throws Exception {
        final Sail sail = GeoRyaSailFactory.getInstance(conf);
        final SailRepositoryConnection conn = new SailRepository(sail).getConnection();
        try {
            populateRya(conn);

            // Only captial
            final String query =
                    "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n"
                            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n"
                            + "SELECT * \n" //
                            + "WHERE { \n" + "  <urn:geo> geo:asWKT ?point .\n"
                            + "  FILTER(geof:sfNear(?point, \"POINT(38.8895 77.0353)\"^^geo:wktLiteral, 100, 1000, 10))"
                            + "}";

            conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
        } finally {
            conn.close();
            sail.shutDown();
        }
    }

    private void populateRya(final SailRepositoryConnection conn) throws Exception {
        // geo 2x2 points
        conn.begin();
        RyaStatement stmnt = statement(WASHINGTON_MONUMENT);
        Statement statement = RyaToRdfConversions.convertStatement(stmnt);
        conn.add(statement);

        stmnt = statement(LINCOLN_MEMORIAL);
        statement = RyaToRdfConversions.convertStatement(stmnt);
        conn.add(statement);

        stmnt = statement(CAPITAL_BUILDING);
        statement = RyaToRdfConversions.convertStatement(stmnt);
        conn.add(statement);

        stmnt = statement(WHITE_HOUSE);
        statement = RyaToRdfConversions.convertStatement(stmnt);
        conn.add(statement);
        conn.commit();
    }

    private static Geometry bindingToGeo(final BindingSet bs) throws ParseException {
        final WKTReader w = new WKTReader();
        return w.read(bs.getValue("point").stringValue());
    }

    private static RyaStatement statement(final Geometry geo) {
        final ValueFactory vf = new ValueFactoryImpl();
        final Resource subject = vf.createURI("urn:geo");
        final URI predicate = GeoConstants.GEO_AS_WKT;
        final WKTWriter w = new WKTWriter();
        final Value object = vf.createLiteral(w.write(geo), GeoConstants.XMLSCHEMA_OGC_WKT);
        return RdfToRyaConversions.convertStatement(new StatementImpl(subject, predicate, object));
    }

}
