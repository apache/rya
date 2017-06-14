/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.geotemporal.accumulo;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.experimental.AccumuloIndexer;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.GeoRyaSailFactory;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.OptionalConfigUtils;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.function.FunctionRegistry;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class AccumuloGeoTemporalIndexIT extends AccumuloITBase {
    private static final String URI_PROPERTY_AT_TIME = "http://www.w3.org/2006/time#atTime";

    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
    private SailRepositoryConnection conn;
    private AccumuloGeoTemporalIndexer indexer;
    private AccumuloRdfConfiguration ryaConf;
	private static final String RYA_INSTANCE_NAME = "testInstance";

    @Before
    public void setUp() throws Exception{
		ryaConf = new AccumuloRdfConfiguration();
		ryaConf.setTablePrefix(RYA_INSTANCE_NAME);
		ryaConf.set(ConfigUtils.CLOUDBASE_USER, super.getUsername());
		ryaConf.set(ConfigUtils.CLOUDBASE_PASSWORD, super.getPassword());
		ryaConf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, super.getZookeepers());
		ryaConf.set(ConfigUtils.CLOUDBASE_INSTANCE, super.getInstanceName());
		ryaConf.set(OptionalConfigUtils.USE_GEO, "true");//"false");
		ryaConf.set(OptionalConfigUtils.USE_GEOTEMPORAL, "true");
		ryaConf.set(OptionalConfigUtils.USE_TEMPORAL, "true");
		ryaConf.set(OptionalConfigUtils.USE_MONGO, "false");
		ryaConf.set(OptionalConfigUtils.TEMPORAL_PREDICATES_LIST,URI_PROPERTY_AT_TIME);
		ryaConf.set(OptionalConfigUtils.GEO_PREDICATES_LIST,GeoConstants.GEO_AS_WKT.stringValue());
        final Sail sail = GeoRyaSailFactory.getInstance(ryaConf);
        conn = new SailRepository(sail).getConnection();
        conn.begin();

        indexer = new AccumuloGeoTemporalIndexer();
        indexer.setConf(ryaConf);
        indexer.init();

//        // the indexer will have been created by the sail->DAO above.  Find it and use it here.
//        List<AccumuloIndexer> indexes = sail.???.getAdditionalIndexers();
//        for (AccumuloIndexer i : indexes) {
//        	if (i instanceof AccumuloGeoTemporalIndexer) {
//        		this.indexer = (AccumuloGeoTemporalIndexer) i;
//        		break;
//        	}
//        }
//        if (this.indexer == null){
//        	throw new Error("AccumuloGeoTemporalIndexer not found from DAO settup during test initialization.");
//        }
//        indexer = new AccumuloGeoTemporalIndexer();
    }

    @Test
    public void ensureInEventStore_Test() throws Exception {
        addStatements();

        final EventStorage eventStorage = indexer.getEventStorage(ryaConf);
        final RyaURI subject = new RyaURI("urn:event1");
        final Optional<Event> event = eventStorage.get(subject);
        System.out.println("Expected subj="+subject+" found event:"+event);
        assertTrue("Should be in the store.", event.isPresent());
        assertTrue("Should have a instant.", event.get().getInstant().isPresent());
        assertTrue("Geo should exist, Both geo and time were added. "+event.get().getGeometry().get(),event.get().getGeometry().isPresent());
        assertFalse("Did not set an interval.",event.get().getInterval().isPresent());
    }


    @Test
    public void ensureStoreTimeOnly_Test() throws Exception {
        addStatements();

        final EventStorage eventStorage = indexer.getEventStorage(ryaConf);
        final RyaURI subject = new RyaURI("urn:event3");
        final Optional<Event> event = eventStorage.get(subject);
        System.out.println("Expected subj="+subject+" found event:"+event);
        assertTrue("Should be in the store.", event.isPresent());
        assertTrue("Should have a instant.", event.get().getInstant().isPresent());
        assertFalse("Geo should be null, since only a time was added. ",event.get().getGeometry().isPresent());
        assertFalse("Did not set an interval.",event.get().getInterval().isPresent());
    }

    @Test
    public void ensureGeoOnlyStore_Test() throws Exception {
        addStatements();

        final EventStorage eventStorage = indexer.getEventStorage(ryaConf);
        final RyaURI subject = new RyaURI("urn:event4");
        final Optional<Event> event = eventStorage.get(subject);
        System.out.println("Expected subj="+subject+" found event:"+event);
        assertTrue("Should be in the store.", event.isPresent());
        assertFalse("Should NOT have an instant.", event.get().getInstant().isPresent());
        assertTrue("Geo should exist, geo only was added. "+event.get().getGeometry().get(),event.get().getGeometry().isPresent());
        assertFalse("Did not set an interval.",event.get().getInterval().isPresent());
    }

    @Test
    public void ensureGeoOnlyMagicDateStore_Test() throws Exception {
        addStatements();

        final String query =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT * "
              + "WHERE { "
                + "  <urn:event4> time:atTime ?time . "
                + "  FILTER(tempo:equals(?time, \""+AccumuloEventStorage.UNKNOWN_DATE.toString()+"\")) "
              + "}";
        //System.out.println("ensureGeoOnlyMagicDateStore_Test Query="+query);
        final TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
        final Set<BindingSet> results = new HashSet<>();
        while(rez.hasNext()) {
            System.out.println("ensureGeoOnlyMagicDateSkip_Test found result:"+rez);
            final BindingSet bs = rez.next();
            results.add(bs);
        }

        assertEquals(0, results.size());
    }


    @Test // TODO this test fails when USE_TEMPORAL="false" with Function not found: http://www.w3.org/2006/time#equals
    public void ensureGeoWithMagicDateStore_Test() throws Exception {
        addStatements();
        final String query =   //fails
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT * "
              + "WHERE { "
              + "  <urn:event6_magicDate> <"+URI_PROPERTY_AT_TIME+"> ?time . "
              + "  FILTER(tempo:equals(?time, \"" + new TemporalInstantRfc3339(AccumuloEventStorage.UNKNOWN_DATE).toString()+"\")) "
              + "}";
         
        System.out.println("ensureGeoOnlyMagicDateStore_Test Query="+query);
        final TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
        final Set<BindingSet> results = new HashSet<>();
        while(rez.hasNext()) {
            System.out.println("ensureGeoWithMagicDateStore_Test found result:" + rez);
            final BindingSet bs = rez.next();
            results.add(bs);
        }

        assertEquals(1, results.size());
    }

	@Test
    public void constantSubjQuery_Test() throws Exception {
        addStatements();
        final String query =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT * "
              + "WHERE { "
                + "  <urn:event1> time:atTime ?time . "
                + "  <urn:event1> geo:asWKT ?point . "
                + "  FILTER(geof:sfWithin(?point, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
                + "  FILTER(tempo:equals(?time, \"2015-12-30T12:00:00Z\")) "
              + "}";

        final TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
        final Set<BindingSet> results = new HashSet<>();
        while(rez.hasNext()) {
            final BindingSet bs = rez.next();
            results.add(bs);
        }
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("point", VF.createLiteral("POINT (0 0)"));
        expected.addBinding("time", VF.createLiteral("2015-12-30T12:00:00Z"));

        assertEquals(1, results.size());
        assertEquals(expected, results.iterator().next());
    }

    /**
     * Tests a query about two points that match both Geo and temporal.
     * Note that a point on the boundry of a polygon is not WITHIN.  It must have a non-empty interior intersection. 
     * @throws Exception
     */
    @Test
    public void variableSubjQuery_Test() throws Exception {
        addStatements();
        final String query =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT * "
              + "WHERE { "
                + "  ?subj time:atTime ?time . "
                + "  ?subj geo:asWKT ?point . "
                + "  FILTER(geof:sfWithin(?point, \"POLYGON((-3 -2, -3 2, 1.1 2, 1.1 -2, -3 -2))\"^^geo:wktLiteral)) "
                + "  FILTER(tempo:equals(?time, \"2015-12-30T12:00:00Z\")) "
              + "}";

        final TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
        final List<BindingSet> results = new ArrayList<>();
        while(rez.hasNext()) {
            final BindingSet bs = rez.next();
            results.add(bs);
        }
        final MapBindingSet expectedZeroZero = new MapBindingSet();
        expectedZeroZero.addBinding("point", VF.createLiteral("POINT (0 0)"));
        expectedZeroZero.addBinding("time", VF.createLiteral("2015-12-30T12:00:00Z"));

        final MapBindingSet expected__OneOne = new MapBindingSet();
        expected__OneOne.addBinding("point", VF.createLiteral("POINT (1 1)"));
        expected__OneOne.addBinding("time", VF.createLiteral("2015-12-30T12:00:00Z"));
        System.out.println("result="+results.get(0));
        
        // Should get exactly two results
        assertEquals(2, results.size());
        // Since the order is not predicted, peek at the first result get(0) so we know which is first.
        int indexOf__OneOne=1;
        int indexOfZeroZero=0;
		if (results.get(  0  ).getBinding("point").getValue().stringValue().contains("1")) {
        	indexOf__OneOne=0;
        	indexOfZeroZero=1;
        } 
        assertEquals("Expect point 0 0 at index="+indexOfZeroZero, expectedZeroZero, results.get(indexOfZeroZero));
        assertEquals("Expect point 1 1 at index="+indexOf__OneOne, expected__OneOne, results.get(indexOf__OneOne));

    }

    private void addStatements() throws Exception {
    	// subj event1 ============
        URI subject = VF.createURI("urn:event1");
        final URI predicate = VF.createURI(URI_PROPERTY_AT_TIME);
        Value object = VF.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        conn.add(VF.createStatement(subject, predicate, object));

        object = VF.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
        conn.add(VF.createStatement(subject, GeoConstants.GEO_AS_WKT, object));

    	// subj event2 ==============
        subject = VF.createURI("urn:event2");
        object = VF.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        conn.add(VF.createStatement(subject, predicate, object));

        object = VF.createLiteral("Point(1 1)", GeoConstants.XMLSCHEMA_OGC_WKT);
        conn.add(VF.createStatement(subject, GeoConstants.GEO_AS_WKT, object));
        
        // event 3 ===== is a time only.  This is an off-label special case in Geomesa!
        subject = VF.createURI("urn:event3");
        object = VF.createLiteral(new TemporalInstantRfc3339(2003, 12, 30, 12, 00, 0).toString());
        conn.add(VF.createStatement(subject, predicate, object));

        // event 4 is a geo only. ============
        subject = VF.createURI("urn:event4");
        object = VF.createLiteral("Point(4 4)", GeoConstants.XMLSCHEMA_OGC_WKT);
        conn.add(VF.createStatement(subject, GeoConstants.GEO_AS_WKT, object));

        // This one matches the time of event1 and event2, but the point is on the , sfwithin will reject it.
        subject = VF.createURI("urn:event5_onBoundry");
        object = VF.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        conn.add(VF.createStatement(subject, predicate, object));

        object = VF.createLiteral("Point(1.1 1.1)", GeoConstants.XMLSCHEMA_OGC_WKT);
        conn.add(VF.createStatement(subject, GeoConstants.GEO_AS_WKT, object));

        // Event 5 is Geo and datetime that matches the fake date used for unknown date.
        subject = VF.createURI("urn:event6_magicDate");
        object = VF.createLiteral(new TemporalInstantRfc3339(AccumuloEventStorage.UNKNOWN_DATE).toString());
        System.out.println("Statement="+VF.createStatement(subject, predicate, object)+" object="+object);
        conn.add(VF.createStatement(subject, predicate, object));

        object = VF.createLiteral("Point(6 6)", GeoConstants.XMLSCHEMA_OGC_WKT);
        conn.add(VF.createStatement(subject, GeoConstants.GEO_AS_WKT, object));
        
    }
}
