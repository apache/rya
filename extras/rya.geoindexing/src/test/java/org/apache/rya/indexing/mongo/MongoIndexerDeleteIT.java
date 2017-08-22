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

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.GeoRyaSailFactory;
import org.apache.rya.indexing.OptionalConfigUtils;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration;
import org.apache.rya.mongodb.MockMongoSingleton;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.mongodb.MongoClient;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKTWriter;

public class MongoIndexerDeleteIT {
    private MongoClient client;
    private Sail sail;
    private SailRepositoryConnection conn;

    @Before
    public void before() throws Exception {
        final MongoIndexingConfiguration indxrConf = MongoIndexingConfiguration.builder()
                .setMongoCollectionPrefix("rya_").setMongoDBName("indexerTests")
            .setUseMongoFreetextIndex(true)
            .setUseMongoTemporalIndex(true)
            .setMongoFreeTextPredicates(RDFS.LABEL.stringValue())
            .setMongoTemporalPredicates("Property:atTime")
            .build();

        client = MockMongoSingleton.getInstance();
        indxrConf.setBoolean(OptionalConfigUtils.USE_GEO, true);
        indxrConf.set(ConfigUtils.GEO_PREDICATES_LIST, "http://www.opengis.net/ont/geosparql#asWKT");
        indxrConf.setBoolean(ConfigUtils.USE_MONGO, true);
        indxrConf.setMongoClient(client);

        sail = GeoRyaSailFactory.getInstance(indxrConf);
        conn = new SailRepository(sail).getConnection();
        conn.begin();
    }

    @After
    public void after() throws Exception {
        if (conn != null) {
            conn.clear();
        }
    }

    @Test
    public void deleteTest() throws Exception {
        populateRya();

        //The extra 1 is from the person type defined in freetext
        assertEquals(8, client.getDatabase("indexerTests").getCollection("rya__triples").count());
        assertEquals(4, client.getDatabase("indexerTests").getCollection("rya_rya_geo").count());
        assertEquals(1, client.getDatabase("indexerTests").getCollection("rya_rya_temporal").count());
        assertEquals(2, client.getDatabase("indexerTests").getCollection("rya_rya_freetext").count());

        //free text -- remove one from many
        String delete = "DELETE DATA \n" //
           + "{\n"
           + "  <urn:people> <http://www.w3.org/2000/01/rdf-schema#label> \"Alice Palace Hose\" "
           + "}";
        Update update = conn.prepareUpdate(QueryLanguage.SPARQL, delete);
        update.execute();

        // temporal -- remove one from one
        delete = "DELETE DATA \n" //
           + "{\n"
           + "  <foo:time> <Property:atTime> \"0001-02-03T04:05:06Z\" "
           + "}";

        update = conn.prepareUpdate(QueryLanguage.SPARQL, delete);
        update.execute();

        //geo -- remove many from many
        delete =
             "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n"
           + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n"
           + "DELETE \n" //
           + "{\n"
           + "  <urn:geo> geo:asWKT ?point \n"
           + "}"
           + "WHERE { \n"
           + "  <urn:geo> geo:asWKT ?point .\n"
           + "  FILTER(geof:sfWithin(?point, \"POLYGON((0 0, 2 0, 2 1, 0 1, 0 0))\"^^geo:wktLiteral))"
           + "}";

        update = conn.prepareUpdate(QueryLanguage.SPARQL, delete);
        update.execute();

        assertEquals(2, client.getDatabase("indexerTests").getCollection("rya_rya_geo").count());
        assertEquals(0, client.getDatabase("indexerTests").getCollection("rya_rya_temporal").count());
        assertEquals(1, client.getDatabase("indexerTests").getCollection("rya_rya_freetext").count());
        assertEquals(4, client.getDatabase("indexerTests").getCollection("rya__triples").count());
    }

    private void populateRya() throws Exception {
        final ValueFactory VF = new ValueFactoryImpl();
        // geo 2x2 points
        final GeometryFactory GF = new GeometryFactory();
        for (int x = 0; x <= 1; x++) {
            for (int y = 0; y <= 1; y++) {
                final Geometry geo = GF.createPoint(new Coordinate(x + .5, y + .5));
                final RyaStatement stmnt = statement(geo);
                final Statement statement = RyaToRdfConversions.convertStatement(stmnt);
                conn.add(statement);
            }
        }

        // freetext
        final URI person = VF.createURI("http://example.org/ontology/Person");
        String uuid;

        uuid = "urn:people";
        conn.add(VF.createURI(uuid), RDF.TYPE, person);
        conn.add(VF.createURI(uuid), RDFS.LABEL, VF.createLiteral("Alice Palace Hose", VF.createURI("http://www.w3.org/2001/XMLSchema#string")));
        conn.add(VF.createURI(uuid), RDFS.LABEL, VF.createLiteral("Bob Snob Hose", "en"));

        // temporal
        final TemporalInstant instant = new TemporalInstantRfc3339(1, 2, 3, 4, 5, 6);
        conn.add(VF.createURI("foo:time"), VF.createURI("Property:atTime"), VF.createLiteral(instant.toString()));
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
