package org.apache.rya.indexing.geoExamples;
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

import java.util.List;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.GeoIndexerType;
import org.apache.rya.indexing.GeoRyaSailFactory;
import org.apache.rya.indexing.accumulo.AccumuloIndexingConfiguration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.OptionalConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;

public class GeowaveDirectExample {
	private static final Logger log = Logger.getLogger(GeowaveDirectExample.class);

	//
	// Connection configuration parameters
	//

	private static final boolean USE_MOCK_INSTANCE = true;
	private static final boolean PRINT_QUERIES = true;
	private static final String INSTANCE = "instance";
	private static final String RYA_TABLE_PREFIX = "x_test_triplestore_";
	private static final String AUTHS = "U";

	public static void main(final String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.set(PrecomputedJoinIndexerConfig.PCJ_STORAGE_TYPE, PrecomputedJoinStorageType.ACCUMULO.name());
		conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, PRINT_QUERIES);
		conf.setBoolean(OptionalConfigUtils.USE_GEO, true);
		conf.setEnum(OptionalConfigUtils.GEO_INDEXER_TYPE, GeoIndexerType.GEO_WAVE);
		
		log.info("Creating the tables as root.");

		SailRepository repository = null;
		SailRepositoryConnection conn = null;

		try {
			log.info("Connecting to Geo Sail Repository.");
			final Sail extSail = GeoRyaSailFactory.getInstance(conf);
			repository = new SailRepository(extSail);
			conn = repository.getConnection();

			final long start = System.currentTimeMillis();
			log.info("Running SPARQL Example: Add Point and Geo Search with PCJ");
			testAddPointAndWithinSearchWithPCJ(conn);
			log.info("Running SPARQL Example: Temporal, Freetext, and Geo Search");
			testTemporalFreeGeoSearch(conn);
			log.info("Running SPARQL Example: Geo, Freetext, and PCJ Search");
			testGeoFreetextWithPCJSearch(conn);
			log.info("Running SPARQL Example: Delete Geo Data");
			testDeleteGeoData(conn);

			log.info("TIME: " + (System.currentTimeMillis() - start) / 1000.);
		} finally {
			log.info("Shutting down");
			closeQuietly(conn);
			closeQuietly(repository);
		}
	}

	private static void closeQuietly(final SailRepository repository) {
		if (repository != null) {
			try {
				repository.shutDown();
			} catch (final RepositoryException e) {
				// quietly absorb this exception
			}
		}
	}

	private static void closeQuietly(final SailRepositoryConnection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (final RepositoryException e) {
				// quietly absorb this exception
			}
		}
	}

	private static Configuration getConf() {

		
		return AccumuloIndexingConfiguration.builder()
			.setUseMockAccumulo(USE_MOCK_INSTANCE)
			.setAuths(AUTHS)
			.setAccumuloUser("root")
			.setAccumuloPassword("")
			.setAccumuloInstance(INSTANCE)
			.setRyaPrefix(RYA_TABLE_PREFIX)
			.setUsePcj(true)
			.setUseAccumuloFreetextIndex(true)
			.setUseAccumuloTemporalIndex(true)
			.build();
		
	}

	

	private static void testAddPointAndWithinSearchWithPCJ(
			final SailRepositoryConnection conn) throws Exception {

		final String update = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "INSERT DATA { " //
				+ "  <urn:feature> a geo:Feature ; " //
				+ "    geo:hasGeometry [ " //
				+ "      a geo:Point ; " //
				+ "      geo:asWKT \"Point(-77.03524 38.889468)\"^^geo:wktLiteral "//
				+ "    ] . " //
				+ "}";

		final Update u = conn.prepareUpdate(QueryLanguage.SPARQL, update);
		u.execute();

		String queryString;
		TupleQuery tupleQuery;
		CountingResultHandler tupleHandler;

		// point outside search ring
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?feature ?point ?wkt " //
				+ "{" //
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-77 39, -76 39, -76 38, -77 38, -77 39))\"^^geo:wktLiteral)) " //
				+ "}";//
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("point outside search ring, Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 0);
		// point inside search ring
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?feature ?point ?wkt " // ?e ?l ?o" //
				+ "{" //
//				+ "  ?feature a ?e . "//
//				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
//				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
				+ "}";//

		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("point inside search ring, Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 1);

		// point inside search ring with Pre-Computed Join
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?feature ?point ?wkt "//?e ?l ?o" //
				+ "{" //
//				+ "  ?feature a ?e . "//
//				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
//				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
				+ "}";//

		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("point inside search ring with Pre-Computed Join, Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() >= 1); // may see points from
														// during previous runs

		// point outside search ring with PCJ
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?feature ?point ?wkt "//?e ?l ?o " //
				+ "{" //
//				+ "  ?feature a ?e . "//
//				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
//				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-77 39, -76 39, -76 38, -77 38, -77 39))\"^^geo:wktLiteral)) " //
				+ "}";//
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("point outside search ring with PCJ, Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 0);

		// point inside search ring with different Pre-Computed Join
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?feature ?point "//?wkt ?e ?c ?l ?o " //
				+ "{" //
//				+ "  ?e a ?c . "//
//				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				//+ "  ?e <uri:talksTo> ?o . "//
				//+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
				+ "}";//
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("point inside search ring with different Pre-Computed Join, Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 1);
	}

	private static void testTemporalFreeGeoSearch(
			final SailRepositoryConnection conn)
			throws MalformedQueryException, RepositoryException,
			UpdateExecutionException, TupleQueryResultHandlerException,
			QueryEvaluationException {
		// Once upon a time, a meeting happened, in a place and time, attended by 5 paladins and another. 
		final String update = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "PREFIX time: <http://www.w3.org/2006/time#> "//
				+ "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> "//
				+ "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "//
				+ "PREFIX ex: <http://example.com/#> "//
				+ "INSERT DATA { " //
				+ "  ex:feature719 a geo:Feature ; " //
				+ "    geo:hasGeometry [ " //
				+ "      a geo:Point ; " //
				+ "      geo:asWKT \"Point(-77.03524 38.889468)\"^^geo:wktLiteral "//
				+ "    ] . "//
				+ "  ex:event719 a time:Instant ;" //
				+ "          time:inXSDDateTime  '2001-01-01T01:01:04-08:00' ;" // 4 seconds
				+ "          ex:locatedAt ex:feature719 ;" //
				+ "			 ex:attendee ex:person01;" //
				+ "			 ex:attendee ex:person02;" //
				+ "			 ex:attendee ex:person03;" //
				+ "			 ex:attendee [a ex:Person ; rdfs:label 'Paladin Ogier the Dane' ] ;" // Use a blank node instead of person04
				+ "			 ex:attendee ex:person05;" //
				+ "			 ex:attendee ex:person06." //
				+ "  ex:person01 a ex:Person ;" //
				+ "          rdfs:label \"Paladin Fossil\"." //
				+ "  ex:person02 a ex:Person ;" //
				+ "          rdfs:label \"Paladin Paul Denning\"." //
				+ "  ex:person03 a ex:Person ;" //
				+ "          rdfs:label 'Paladin Will Travel'." //
				+ "  ex:person05 a ex:Person ;" //
				+ "          rdfs:label 'Paladin dimethyl disulfide'." //
				+ "  ex:person06 a ex:Person ;" //
				+ "          rdfs:label 'Ignore me'." //
				+ "" //
				+ "}";
		final Update u = conn.prepareUpdate(QueryLanguage.SPARQL, update);
		u.execute();

		String queryString;
		TupleQuery tupleQuery;
		CountingResultHandler tupleHandler;

		// Find all events after a time, located in a polygon square, whose attendees have label names beginning with "Pal"
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "PREFIX time: <http://www.w3.org/2006/time#> "//
				+ "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> "//
				+ "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
				+ "PREFIX ex: <http://example.com/#>  "//
				+ "SELECT ?feature ?point ?wkt ?event ?time ?person ?match" //
				+ "{" //
				+ "	 ?event a  time:Instant ; \n"//
				+ "    time:inXSDDateTime ?time ; \n"//
				+ "    ex:locatedAt ?feature ;" //
				+ "    ex:attendee ?person." //
				+ "  FILTER(tempo:after(?time, '2001-01-01T01:01:03-08:00') ) \n"// after 3  seconds
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)). " //
				+ "  ?person a ex:Person . "//
				+ "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?match . "//
				+ "  FILTER(fts:text(?match, \"Pal*\")) " //
				+ "}";//

		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 5 ); 

	}

	private static void testGeoFreetextWithPCJSearch(
			final SailRepositoryConnection conn)
			throws MalformedQueryException, RepositoryException,
			TupleQueryResultHandlerException, QueryEvaluationException {
		// ring outside point
		final String queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?feature ?point ?wkt ?e ?c ?l ?o ?person ?match " //
				+ "{" //
				+ "  ?person a <http://example.org/ontology/Person> . "//
				+ "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?match . "//
				+ "  FILTER(fts:text(?match, \"!alice & hose\")) " //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
				+ "}";//
		final TupleQuery tupleQuery = conn.prepareTupleQuery(
				QueryLanguage.SPARQL, queryString);
		final CountingResultHandler tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 0);// TODO ==1  some data is missing for this query!
	}



	private static void testDeleteGeoData(final SailRepositoryConnection conn)
			throws Exception {
		// Delete all stored points
		final String sparqlDelete = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "DELETE {\n" //
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "}\n" + "WHERE { \n" + "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "}";//

		final Update deleteUpdate = conn.prepareUpdate(QueryLanguage.SPARQL,
				sparqlDelete);
		deleteUpdate.execute();

		String queryString;
		TupleQuery tupleQuery;
		CountingResultHandler tupleHandler;

		// Find all stored points
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?feature ?point ?wkt " //
				+ "{" //
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "}";//
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 0);
	}

	private static class CountingResultHandler implements
			TupleQueryResultHandler {
		private int count = 0;

		public int getCount() {
			return count;
		}

		public void resetCount() {
			count = 0;
		}

		@Override
		public void startQueryResult(final List<String> arg0)
				throws TupleQueryResultHandlerException {
		}

		@Override
		public void handleSolution(final BindingSet arg0)
				throws TupleQueryResultHandlerException {
			count++;
			System.out.println(arg0);
		}

		@Override
		public void endQueryResult() throws TupleQueryResultHandlerException {
		}

		@Override
		public void handleBoolean(final boolean arg0)
				throws QueryResultHandlerException {
		}

		@Override
		public void handleLinks(final List<String> arg0)
				throws QueryResultHandlerException {
		}
	}
}
