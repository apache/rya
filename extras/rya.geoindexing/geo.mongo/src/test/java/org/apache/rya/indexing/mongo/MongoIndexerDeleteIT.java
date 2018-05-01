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
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.OptionalConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoRyaITBase;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKTWriter;

public class MongoIndexerDeleteIT extends MongoRyaITBase {
    @Override
    public void updateConfiguration(final MongoDBRdfConfiguration conf) {
        conf.setStrings(ConfigUtils.FREETEXT_PREDICATES_LIST, new String[] {RDFS.LABEL.stringValue()});
        conf.setStrings(ConfigUtils.TEMPORAL_PREDICATES_LIST, new String[] {"Property:atTime"});
        conf.setBoolean(ConfigUtils.USE_FREETEXT, true);
        conf.setBoolean(ConfigUtils.USE_TEMPORAL, true);
        conf.setBoolean(OptionalConfigUtils.USE_GEO, true);
        conf.set(ConfigUtils.GEO_PREDICATES_LIST, "http://www.opengis.net/ont/geosparql#asWKT");
    }

    @Test
    public void deleteTest() throws Exception {
        final Sail sail = GeoRyaSailFactory.getInstance(conf);
        final SailRepositoryConnection conn = new SailRepository(sail).getConnection();
        try {
            populateRya(conn);
            final MongoClient client = conf.getMongoClient();

            //The extra 1 is from the person type defined in freetext
            assertEquals(8, client.getDatabase(conf.getMongoDBName()).getCollection(conf.getTriplesCollectionName()).count());
            assertEquals(4, client.getDatabase(conf.getMongoDBName()).getCollection("ryatest_geo").count());
            assertEquals(1, client.getDatabase(conf.getMongoDBName()).getCollection("ryatest_temporal").count());
            assertEquals(2, client.getDatabase(conf.getMongoDBName()).getCollection("ryatest_freetext").count());

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

            assertEquals(2, client.getDatabase(conf.getMongoDBName()).getCollection("ryatest_geo").count());
            assertEquals(0, client.getDatabase(conf.getMongoDBName()).getCollection("ryatest_temporal").count());
            assertEquals(1, client.getDatabase(conf.getMongoDBName()).getCollection("ryatest_freetext").count());
            assertEquals(4, client.getDatabase(conf.getMongoDBName()).getCollection(conf.getTriplesCollectionName()).count());
        } finally {
            conn.close();
            sail.shutDown();
        }
    }

    private void populateRya(final SailRepositoryConnection conn) throws Exception {
        final ValueFactory vf = SimpleValueFactory.getInstance();
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
        final IRI person = vf.createIRI("http://example.org/ontology/Person");
        String uuid;

        uuid = "urn:people";
        conn.add(vf.createIRI(uuid), RDF.TYPE, person);
        conn.add(vf.createIRI(uuid), RDFS.LABEL, vf.createLiteral("Alice Palace Hose", vf.createIRI("http://www.w3.org/2001/XMLSchema#string")));
        conn.add(vf.createIRI(uuid), RDFS.LABEL, vf.createLiteral("Bob Snob Hose"));

        // temporal
        final TemporalInstant instant = new TemporalInstantRfc3339(1, 2, 3, 4, 5, 6);
        conn.add(vf.createIRI("foo:time"), vf.createIRI("Property:atTime"), vf.createLiteral(instant.toString()));
        conn.commit();
    }

    private static RyaStatement statement(final Geometry geo) {
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Resource subject = vf.createIRI("urn:geo");
        final IRI predicate = GeoConstants.GEO_AS_WKT;
        final WKTWriter w = new WKTWriter();
        final Value object = vf.createLiteral(w.write(geo), GeoConstants.XMLSCHEMA_OGC_WKT);
        return RdfToRyaConversions.convertStatement(vf.createStatement(subject, predicate, object));
    }

}
