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

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.GeoRyaSailFactory;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.OptionalConfigUtils;
import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration;
import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration.MongoDBIndexingConfigBuilder;
import org.apache.rya.mongodb.EmbeddedMongoFactory;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class RyaMongoGeoDirectExample {
    private static final Logger log = Logger.getLogger(RyaMongoGeoDirectExample.class);

    //
    // Connection configuration parameters
    //

    private static final boolean PRINT_QUERIES = true;
    private static final String MONGO_DB = "rya";
    private static final String MONGO_COLL_PREFIX = "rya_";
    private static final boolean USE_MOCK = true;
    private static final boolean USE_INFER = true;
    private static final String MONGO_INSTANCE_URL = "localhost";
    private static final String MONGO_INSTANCE_PORT = "27017";

    public static void main(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, PRINT_QUERIES);
		conf.setBoolean(OptionalConfigUtils.USE_GEO, true);  // Note also the use of "GeoRyaSailFactory" below.
		conf.setStrings(OptionalConfigUtils.GEO_PREDICATES_LIST, "http://www.opengis.net/ont/geosparql#asWKT");  // Note also the use of "GeoRyaSailFactory" below.
  
        SailRepository repository = null;
        SailRepositoryConnection conn = null;
        try {
            log.info("Connecting to Indexing Sail Repository.");
            Sail sail = GeoRyaSailFactory.getInstance(conf);
            repository = new SailRepository(sail);
            conn = repository.getConnection();

            long start = System.currentTimeMillis();
            testAddPointAndWithinSearch(conn);  // uses geospatial features

            log.info("TIME: " + (System.currentTimeMillis() - start) / 1000.);
        } finally {
            log.info("Shutting down");
            closeQuietly(conn);
            closeQuietly(repository);
            if (mock != null) {
                mock.shutdown();
            }
        }
    }
/**
 * Try out some geospatial data and queries
 * @param repository
 */
    private static void testAddPointAndWithinSearch(SailRepositoryConnection conn) throws Exception {

        String update = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
                + "INSERT DATA { " //
                + "  <urn:feature> a geo:Feature ; " //
                + "    geo:hasGeometry [ " //
                + "      a geo:Point ; " //
                + "      geo:asWKT \"Point(-77.03524 38.889468)\"^^geo:wktLiteral "//
                + "    ] . " //
                + "}";

        Update u = conn.prepareUpdate(QueryLanguage.SPARQL, update);
        u.execute();

        String queryString;
        TupleQuery tupleQuery;
        CountingResultHandler tupleHandler;

        // ring containing point
        queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
                + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
                + "SELECT ?feature ?point ?wkt " //
                + "{" //
                + "  ?feature a geo:Feature . "//
                + "  ?feature geo:hasGeometry ?point . "//
                + "  ?point a geo:Point . "//
                + "  ?point geo:asWKT ?wkt . "//
                + "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
                + "}";//
        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleHandler = new CountingResultHandler();
        tupleQuery.evaluate(tupleHandler);
        log.info("Result count -- ring containing point: " + tupleHandler.getCount());
        Validate.isTrue(tupleHandler.getCount() >= 1); // may see points from during previous runs

        // ring outside point
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
        log.info("Result count -- ring outside point: " + tupleHandler.getCount());
        Validate.isTrue(tupleHandler.getCount() == 0);
    }

    private static void closeQuietly(SailRepository repository) {
        if (repository != null) {
            try {
                repository.shutDown();
            } catch (RepositoryException e) {
                // quietly absorb this exception
            }
        }
    }

    private static void closeQuietly(SailRepositoryConnection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (RepositoryException e) {
                // quietly absorb this exception
            }
        }
    }

    private static EmbeddedMongoFactory mock = null;
    private static Configuration getConf() throws IOException {

    	MongoDBIndexingConfigBuilder builder = MongoIndexingConfiguration.builder()
    		.setUseMockMongo(USE_MOCK).setUseInference(USE_INFER).setAuths("U");

        if (USE_MOCK) {
            mock = EmbeddedMongoFactory.newFactory();
            MongoClient c = mock.newMongoClient();
            ServerAddress address = c.getAddress();
            String url = address.getHost();
            String port = Integer.toString(address.getPort());
            c.close();
            builder.setMongoHost(url).setMongoPort(port);
        } else {
            // User name and password must be filled in:
        	builder = builder.setMongoUser("fill this in")
        					 .setMongoPassword("fill this in")
        					 .setMongoHost(MONGO_INSTANCE_URL)
        					 .setMongoPort(MONGO_INSTANCE_PORT);
        }
        
        return builder.setMongoDBName(MONGO_DB)
               .setMongoCollectionPrefix(MONGO_COLL_PREFIX)
               .setUseMongoFreetextIndex(true)
               .setMongoFreeTextPredicates(RDFS.LABEL.stringValue()).build();
        
    }


    private static class CountingResultHandler implements TupleQueryResultHandler {
        private int count = 0;

        public int getCount() {
            return count;
        }

        @Override
        public void startQueryResult(List<String> arg0) throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleSolution(BindingSet arg0) throws TupleQueryResultHandlerException {
            count++;
            System.out.println(arg0);
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleBoolean(boolean arg0) throws QueryResultHandlerException {
          // TODO Auto-generated method stub
          
        }

        @Override
        public void handleLinks(List<String> arg0) throws QueryResultHandlerException {
          // TODO Auto-generated method stub
          
        }
    }
}
