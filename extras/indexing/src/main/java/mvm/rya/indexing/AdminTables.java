//package mvm.rya.indexing;

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
import mvm.rya.indexing.external.tupleSet.PcjTables;
import mvm.rya.indexing.external.tupleSet.PcjTables.PcjException;
import mvm.rya.indexing.external.tupleSet.PcjTables.PcjVarOrderFactory;
import mvm.rya.rdftriplestore.inference.InferenceEngineException;
import mvm.rya.sail.config.RyaSailFactory;

public class AdminTables {
	
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
			/*log.info("Running SPARQL Example: Add and Delete");
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
			 */
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
		
		/*  add these in later
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
				
		*/
		return conf;
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
	

}
