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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjVarOrderFactory;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.google.common.base.Optional;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.accumulo.geo.GeoConstants;
import mvm.rya.rdftriplestore.inference.InferenceEngineException;
import mvm.rya.sail.config.RyaSailFactory;

public class RyaDirectExample {
	private static final Logger log = Logger.getLogger(RyaDirectExample.class);

	//
	// Connection configuration parameters
	//

	private static final boolean USE_MOCK_INSTANCE = true;
	private static final boolean PRINT_QUERIES = true;
	private static final String INSTANCE = "instance";
	private static final String RYA_TABLE_PREFIX = "x_test_triplestore_";
	private static final String AUTHS = "";

	public static void main(String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, PRINT_QUERIES);

		log.info("Creating the tables as root.");
		// createTables(addRootConf(conf), conf);

		SailRepository repository = null;
		SailRepositoryConnection conn = null;

		try {
			log.info("Connecting to Indexing Sail Repository.");

			final Sail extSail = RyaSailFactory.getInstance(conf);
			repository = new SailRepository(extSail);
			repository.initialize();
			conn = repository.getConnection();

			createPCJ(conf);

			final long start = System.currentTimeMillis();
			log.info("Running SPARQL Example: Add and Delete");
			testAddAndDelete(conn);
			log.info("Running SAIL/SPARQL Example: PCJ Search");
			testPCJSearch(conn);
			log.info("Running SAIL/SPARQL Example: Add and Temporal Search");
			testAddAndTemporalSearchWithPCJ(conn);
			log.info("Running SAIL/SPARQL Example: Add and Free Text Search with PCJ");
			testAddAndFreeTextSearchWithPCJ(conn);
			log.info("Running SPARQL Example: Add Point and Geo Search with PCJ");
			testAddPointAndWithinSearchWithPCJ(conn);
			log.info("Running SPARQL Example: Temporal, Freetext, and Geo Search");
			testTemporalFreeGeoSearch(conn);
			log.info("Running SPARQL Example: Geo, Freetext, and PCJ Search");
			testGeoFreetextWithPCJSearch(conn);
			log.info("Running SPARQL Example: Delete Temporal Data");
			testDeleteTemporalData(conn);
			log.info("Running SPARQL Example: Delete Free Text Data");
			testDeleteFreeTextData(conn);
			log.info("Running SPARQL Example: Delete Geo Data");
			testDeleteGeoData(conn);

			log.info("TIME: " + (System.currentTimeMillis() - start) / 1000.);
		} finally {
			log.info("Shutting down");
			closeQuietly(conn);
			closeQuietly(repository);
		}
	}

	private static void closeQuietly(SailRepository repository) {
		if (repository != null) {
			try {
				repository.shutDown();
			} catch (final RepositoryException e) {
				// quietly absorb this exception
			}
		}
	}

	private static void closeQuietly(SailRepositoryConnection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (final RepositoryException e) {
				// quietly absorb this exception
			}
		}
	}

	private static Configuration getConf() {

		final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

		conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, USE_MOCK_INSTANCE);
		conf.set(ConfigUtils.USE_PCJ, "true");
		conf.set(ConfigUtils.USE_GEO, "true");
		conf.set(ConfigUtils.USE_FREETEXT, "true");
		conf.set(ConfigUtils.USE_TEMPORAL, "true");
		conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX,
				RYA_TABLE_PREFIX);
		conf.set(ConfigUtils.CLOUDBASE_USER, "root");
		conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
		conf.set(ConfigUtils.CLOUDBASE_INSTANCE, INSTANCE);
		conf.setInt(ConfigUtils.NUM_PARTITIONS, 3);
		conf.set(ConfigUtils.CLOUDBASE_AUTHS, AUTHS);

		// only geo index statements with geo:asWKT predicates
		conf.set(ConfigUtils.GEO_PREDICATES_LIST,
				GeoConstants.GEO_AS_WKT.stringValue());
		return conf;
	}

	public static void testAddAndDelete(SailRepositoryConnection conn)
			throws MalformedQueryException, RepositoryException,
			UpdateExecutionException, QueryEvaluationException,
			TupleQueryResultHandlerException, AccumuloException,
			AccumuloSecurityException, TableNotFoundException {

		// Add data
		String query = "INSERT DATA\n"//
				+ "{ GRAPH <http://updated/test> {\n"//
				+ "  <http://acme.com/people/Mike> " //
				+ "       <http://acme.com/actions/likes> \"A new book\" ;\n"//
				+ "       <http://acme.com/actions/likes> \"Avocados\" .\n"
				+ "} }";

		log.info("Performing Query");

		Update update = conn.prepareUpdate(QueryLanguage.SPARQL, query);
		update.execute();

		query = "select ?p ?o { GRAPH <http://updated/test> {<http://acme.com/people/Mike> ?p ?o . }}";
		final CountingResultHandler resultHandler = new CountingResultHandler();
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL,
				query);
		tupleQuery.evaluate(resultHandler);
		log.info("Result count : " + resultHandler.getCount());

		Validate.isTrue(resultHandler.getCount() == 2);
		resultHandler.resetCount();

		// Delete Data
		query = "DELETE DATA\n" //
				+ "{ GRAPH <http://updated/test> {\n"
				+ "  <http://acme.com/people/Mike> <http://acme.com/actions/likes> \"A new book\" ;\n"
				+ "   <http://acme.com/actions/likes> \"Avocados\" .\n" + "}}";

		update = conn.prepareUpdate(QueryLanguage.SPARQL, query);
		update.execute();

		query = "select ?p ?o { GRAPH <http://updated/test> {<http://acme.com/people/Mike> ?p ?o . }}";
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
		tupleQuery.evaluate(resultHandler);
		log.info("Result count : " + resultHandler.getCount());

		Validate.isTrue(resultHandler.getCount() == 0);
	}

	private static void testPCJSearch(SailRepositoryConnection conn)
			throws Exception {

		String queryString;
		TupleQuery tupleQuery;
		CountingResultHandler tupleHandler;

		// ///////////// search for bob
		queryString = "SELECT ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "}";//

		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 1);

		// ///////////// search for bob
		queryString = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
				+ "SELECT ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?c a ?e . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "}";//

		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 2);

	}

	private static void testAddAndTemporalSearchWithPCJ(
			SailRepositoryConnection conn) throws Exception {

		// create some resources and literals to make statements out of

		final String sparqlInsert = "PREFIX time: <http://www.w3.org/2006/time#>\n"
				+ "INSERT DATA {\n" //
				+ "_:eventz       a       time:Instant ;\n"
				+ "     time:inXSDDateTime '2001-01-01T01:01:01-08:00' ;\n" // one
																			// second
				+ "     time:inXSDDateTime '2001-01-01T04:01:02.000-05:00'^^<http://www.w3.org/2001/XMLSchema#dateTime> ;\n" // 2
																																// seconds
				+ "     time:inXSDDateTime \"2001-01-01T01:01:03-08:00\" ;\n" // 3
																				// seconds
				+ "     time:inXSDDateTime '2001-01-01T01:01:04-08:00' ;\n" // 4
																			// seconds
				+ "     time:inXSDDateTime '2001-01-01T09:01:05Z' ;\n"
				+ "     time:inXSDDateTime '2006-01-01' ;\n"
				+ "     time:inXSDDateTime '2007-01-01' ;\n"
				+ "     time:inXSDDateTime '2008-01-01' ; .\n" + "}";

		final Update update = conn.prepareUpdate(QueryLanguage.SPARQL, sparqlInsert);
		update.execute();

		// Find all stored dates.
		String queryString = "PREFIX time: <http://www.w3.org/2006/time#> \n"//
				+ "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"//
				+ "SELECT ?event ?time \n" //
				+ "WHERE { \n"
				+ "  ?event time:inXSDDateTime ?time . \n"//
				+ "  FILTER(tempo:after(?time, '2001-01-01T01:01:03-08:00') ) \n"// after
																					// 3
																					// seconds
				+ "}";//

		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL,
				queryString);
		CountingResultHandler tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 5);

		// Find all stored dates.
		queryString = "PREFIX time: <http://www.w3.org/2006/time#> \n"//
				+ "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"//
				+ "SELECT ?event ?time \n" //
				+ "WHERE { \n"
				+ "  ?event time:inXSDDateTime ?time . \n"//
				+ "  ?event a  time:Instant . \n"//
				+ "  FILTER(tempo:after(?time, '2001-01-01T01:01:03-08:00') ) \n"// after
																					// 3
																					// seconds
				+ "}";//

		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 5);

		// Find all stored dates.
		queryString = "PREFIX time: <http://www.w3.org/2006/time#> \n"//
				+ "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"//
				+ "SELECT ?event ?time ?e ?c ?l ?o \n" //
				+ "WHERE { \n"
				+ "  ?e a ?c . \n"//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . \n"//
				+ "  ?e <uri:talksTo> ?o . \n"//
				+ "  ?event a  time:Instant . \n"//
				+ "  ?event time:inXSDDateTime ?time . \n"//
				+ "  FILTER(tempo:after(?time, '2001-01-01T01:01:03-08:00') ) \n"// after
																					// 3
																					// seconds
				+ "}";//

		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 5);
	}

	private static void testAddAndFreeTextSearchWithPCJ(
			SailRepositoryConnection conn) throws Exception {
		// add data to the repository using the SailRepository add methods
		final ValueFactory f = conn.getValueFactory();
		final URI person = f.createURI("http://example.org/ontology/Person");

		String uuid;

		uuid = "urn:people:alice";
		conn.add(f.createURI(uuid), RDF.TYPE, person);
		conn.add(f.createURI(uuid), RDFS.LABEL,
				f.createLiteral("Alice Palace Hose", f.createURI("xsd:string")));

		uuid = "urn:people:bobss";
		conn.add(f.createURI(uuid), RDF.TYPE, person);
		conn.add(f.createURI(uuid), RDFS.LABEL,
				f.createLiteral("Bob Snob Hose", "en"));

		String queryString;
		TupleQuery tupleQuery;
		CountingResultHandler tupleHandler;

		// ///////////// search for alice
		queryString = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
				+ "SELECT ?person ?match ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?match . "//
				+ "  FILTER(fts:text(?match, \"pal*\")) " //
				+ "}";//
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 1);

		// ///////////// search for alice and bob
		queryString = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
				+ "SELECT ?person ?match " //
				+ "{" //
				+ "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?match . "//
				+ "  ?person a <http://example.org/ontology/Person> . "//
				+ "  FILTER(fts:text(?match, \"(alice | bob) *SE\")) " //
				+ "}";//
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 2);

		// ///////////// search for alice and bob
		queryString = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
				+ "SELECT ?person ?match " //
				+ "{" //
				+ "  ?person a <http://example.org/ontology/Person> . "//
				+ "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?match . "//
				+ "  FILTER(fts:text(?match, \"(alice | bob) *SE\")) " //
				+ "  FILTER(fts:text(?match, \"pal*\")) " //
				+ "}";//
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 1);

		// ///////////// search for bob
		queryString = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
				+ "SELECT ?person ?match ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?person a <http://example.org/ontology/Person> . "//
				+ "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?match . "//
				+ "  FILTER(fts:text(?match, \"!alice & hose\")) " //
				+ "}";//

		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 1);
	}

	private static void testAddPointAndWithinSearchWithPCJ(
			SailRepositoryConnection conn) throws Exception {

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
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 0);

		// point inside search ring
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?feature ?point ?wkt ?e ?l ?o" //
				+ "{" //
				+ "  ?feature a ?e . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
				+ "}";//

		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 1);

		// point inside search ring with Pre-Computed Join
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?feature ?point ?wkt ?e ?l ?o" //
				+ "{" //
				+ "  ?feature a ?e . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
				+ "}";//

		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() >= 1); // may see points from
														// during previous runs

		// point outside search ring with PCJ
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?feature ?point ?wkt ?e ?l ?o " //
				+ "{" //
				+ "  ?feature a ?e . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-77 39, -76 39, -76 38, -77 38, -77 39))\"^^geo:wktLiteral)) " //
				+ "}";//
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 0);

		// point inside search ring with different Pre-Computed Join
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?feature ?point ?wkt ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
				+ "}";//
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 1);
	}

	private static void testTemporalFreeGeoSearch(SailRepositoryConnection conn)
			throws MalformedQueryException, RepositoryException,
			UpdateExecutionException, TupleQueryResultHandlerException,
			QueryEvaluationException {

		String queryString;
		TupleQuery tupleQuery;
		CountingResultHandler tupleHandler;

		// ring containing point
		queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "PREFIX time: <http://www.w3.org/2006/time#> "//
				+ "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> "//
				+ "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
				+ "SELECT ?feature ?point ?wkt ?event ?time ?person ?match" //
				+ "{" //
				+ "  ?event a  time:Instant . \n"//
				+ "  ?event time:inXSDDateTime ?time . \n"//
				+ "  FILTER(tempo:after(?time, '2001-01-01T01:01:03-08:00') ) \n"// after
																					// 3
																					// seconds
				+ "  ?feature a geo:Feature . "//
				+ "  ?feature geo:hasGeometry ?point . "//
				+ "  ?point a geo:Point . "//
				+ "  ?point geo:asWKT ?wkt . "//
				+ "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)). " //
				+ "  ?person a <http://example.org/ontology/Person> . "//
				+ "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?match . "//
				+ "  FILTER(fts:text(?match, \"pal*\")) " //
				+ "}";//

		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 5);

	}

	private static void testGeoFreetextWithPCJSearch(
			SailRepositoryConnection conn) throws MalformedQueryException,
			RepositoryException, TupleQueryResultHandlerException,
			QueryEvaluationException {
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
		final TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL,
				queryString);
		final CountingResultHandler tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 1);
	}

	private static void testDeleteTemporalData(SailRepositoryConnection conn)
			throws Exception {
		// Delete all stored dates
		final String sparqlDelete = "PREFIX time: <http://www.w3.org/2006/time#>\n"
				+ "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"//
				+ "DELETE {\n" //
				+ "  ?event time:inXSDDateTime ?time . \n" + "}\n"
				+ "WHERE { \n" + "  ?event time:inXSDDateTime ?time . \n"//
				+ "}";//

		final Update deleteUpdate = conn.prepareUpdate(QueryLanguage.SPARQL,
				sparqlDelete);
		deleteUpdate.execute();

		// Find all stored dates.
		final String queryString = "PREFIX time: <http://www.w3.org/2006/time#> \n"//
				+ "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"//
				+ "SELECT ?event ?time \n" //
				+ "WHERE { \n"
				+ "  ?event time:inXSDDateTime ?time . \n"//
				+ "  FILTER(tempo:after(?time, '2001-01-01T01:01:03-08:00') ) \n"// after
																					// 3
																					// seconds
				+ "}";//

		final CountingResultHandler tupleHandler = new CountingResultHandler();
		final TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL,
				queryString);
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 0);
	}

	private static void testDeleteFreeTextData(SailRepositoryConnection conn)
			throws Exception {
		// Delete data from the repository using the SailRepository remove
		// methods
		final ValueFactory f = conn.getValueFactory();
		final URI person = f.createURI("http://example.org/ontology/Person");

		String uuid;

		uuid = "urn:people:alice";
		conn.remove(f.createURI(uuid), RDF.TYPE, person);
		conn.remove(f.createURI(uuid), RDFS.LABEL,
				f.createLiteral("Alice Palace Hose", f.createURI("xsd:string")));

		uuid = "urn:people:bobss";
		conn.remove(f.createURI(uuid), RDF.TYPE, person);
		conn.remove(f.createURI(uuid), RDFS.LABEL,
				f.createLiteral("Bob Snob Hose", "en"));

		conn.remove(person, RDFS.LABEL, f.createLiteral("label", "en"));

		String queryString;
		TupleQuery tupleQuery;
		CountingResultHandler tupleHandler;

		// Find all
		queryString = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
				+ "SELECT ?person ?match " //
				+ "{" //
				+ "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?match . "//
				+ "  ?person a <http://example.org/ontology/Person> . "//
				+ "}";//
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tupleHandler = new CountingResultHandler();
		tupleQuery.evaluate(tupleHandler);
		log.info("Result count : " + tupleHandler.getCount());
		Validate.isTrue(tupleHandler.getCount() == 0);
	}

	private static void testDeleteGeoData(SailRepositoryConnection conn)
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

	private static void createPCJ(Configuration conf)
			throws RepositoryException, AccumuloException,
			AccumuloSecurityException, TableExistsException, PcjException, InferenceEngineException {


		final Configuration config = new AccumuloRdfConfiguration(conf);
		config.set(ConfigUtils.USE_PCJ, "false");
		Sail extSail = null;
		try {
			extSail = RyaSailFactory.getInstance(config);
		} catch (final RyaDAOException e) {
			e.printStackTrace();
		}
		final SailRepository repository = new SailRepository(extSail);
		repository.initialize();
		final SailRepositoryConnection conn = repository.getConnection();


		final String queryString1 = ""//
				+ "SELECT ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?c a ?e . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "}";//

		final String queryString2 = ""//
				+ "SELECT ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "}";//

		URI obj, subclass, talksTo;
		final URI person = new URIImpl("urn:people:alice");
		final URI feature = new URIImpl("urn:feature");
		final URI sub = new URIImpl("uri:entity");
		subclass = new URIImpl("uri:class");
		obj = new URIImpl("uri:obj");
		talksTo = new URIImpl("uri:talksTo");

		conn.add(person, RDF.TYPE, sub);
		conn.add(feature, RDF.TYPE, sub);
		conn.add(sub, RDF.TYPE, subclass);
		conn.add(sub, RDFS.LABEL, new LiteralImpl("label"));
		conn.add(sub, talksTo, obj);

		final String tablename1 = RYA_TABLE_PREFIX + "INDEX_1";
		final String tablename2 = RYA_TABLE_PREFIX + "INDEX_2";

		final Connector accCon = new MockInstance(INSTANCE).getConnector("root",
				new PasswordToken("".getBytes()));

		new PcjTables().createAndPopulatePcj(conn, accCon, tablename1,
				queryString1, new String[] { "e", "c", "l", "o" },
				Optional.<PcjVarOrderFactory> absent());

		new PcjTables().createAndPopulatePcj(conn, accCon, tablename2,
				queryString2, new String[] { "e", "c", "l", "o" },
				Optional.<PcjVarOrderFactory> absent());

	}

	private static class CountingResultHandler implements
			TupleQueryResultHandler {
		private int count = 0;

		public int getCount() {
			return count;
		}

		public void resetCount() {
			this.count = 0;
		}

		@Override
		public void startQueryResult(List<String> arg0)
				throws TupleQueryResultHandlerException {
		}

		@Override
		public void handleSolution(BindingSet arg0)
				throws TupleQueryResultHandlerException {
			count++;
			System.out.println(arg0);
		}

		@Override
		public void endQueryResult() throws TupleQueryResultHandlerException {
		}

		@Override
		public void handleBoolean(boolean arg0)
				throws QueryResultHandlerException {
		}

		@Override
		public void handleLinks(List<String> arg0)
				throws QueryResultHandlerException {
		}
	}
}
