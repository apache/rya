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

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.sail.config.RyaSailFactory;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
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

public class EntityDirectExample {
    private static final Logger log = Logger.getLogger(EntityDirectExample.class);

    //
    // Connection configuration parameters
    //

    private static final boolean USE_MOCK_INSTANCE = true;
    private static final boolean PRINT_QUERIES = true;
    private static final String INSTANCE = "instance";
    private static final String RYA_TABLE_PREFIX = "x_test_triplestore_";
    private static final String AUTHS = "U";

    public static void main(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, PRINT_QUERIES);

        log.info("Creating the tables as root.");
        SailRepository repository = null;
        SailRepositoryConnection conn = null;

        try {
            log.info("Connecting to Indexing Sail Repository.");

            Sail extSail = RyaSailFactory.getInstance(conf);
            repository = new SailRepository(extSail);
            repository.initialize();
            conn = repository.getConnection();

            log.info("Running SPARQL Example: Add and Delete");
            testAddAndDelete(conn);
            log.info("Running SAIL/SPARQL Example: Add and Temporal Search");
            testAddAndTemporalSearchWithPCJ(conn);

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





    public static void testAddAndDelete(SailRepositoryConnection conn) throws MalformedQueryException,
            RepositoryException, UpdateExecutionException, QueryEvaluationException, TupleQueryResultHandlerException,
            AccumuloException, AccumuloSecurityException, TableNotFoundException {

        // Add data
        String query = "INSERT DATA\n"//
                + "{ GRAPH <http://updated/test> {\n"//
                + "  <http://acme.com/people/Mike> " //
                + "       <http://acme.com/actions/likes> \"A new book\" ;\n"//
                + "       <http://acme.com/actions/likes> \"Avocados\" .\n" + "} }";

        log.info("Performing Query");

        Update update = conn.prepareUpdate(QueryLanguage.SPARQL, query);
        update.execute();

        query = "select ?x {GRAPH <http://updated/test> {?x <http://acme.com/actions/likes> \"A new book\" . "//
                + " ?x <http://acme.com/actions/likes> \"Avocados\" }}";
        CountingResultHandler resultHandler = new CountingResultHandler();
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.evaluate(resultHandler);
        log.info("Result count : " + resultHandler.getCount());

        Validate.isTrue(resultHandler.getCount() == 1);
        resultHandler.resetCount();

        // Delete Data
        query = "DELETE DATA\n" //
                + "{ GRAPH <http://updated/test> {\n"
                + "  <http://acme.com/people/Mike> <http://acme.com/actions/likes> \"A new book\" ;\n"
                + "   <http://acme.com/actions/likes> \"Avocados\" .\n" + "}}";

        update = conn.prepareUpdate(QueryLanguage.SPARQL, query);
        update.execute();

        query = "select ?x {GRAPH <http://updated/test> {?x <http://acme.com/actions/likes> \"A new book\" . "//
                + " ?x <http://acme.com/actions/likes> \"Avocados\" }}";
        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.evaluate(resultHandler);
        log.info("Result count : " + resultHandler.getCount());

        Validate.isTrue(resultHandler.getCount() == 0);
    }





    private static void testAddAndTemporalSearchWithPCJ(SailRepositoryConnection conn) throws Exception {

        // create some resources and literals to make statements out of

        String sparqlInsert = "PREFIX pref: <http://www.model/pref#> \n"
                + "INSERT DATA {\n" //
                + "<urn:Bob>       a       pref:Person ;\n" //
                + "     pref:hasProperty1 'property1' ;\n" //  one second
                + "     pref:hasProperty2 'property2' ;\n" //   2 seconds
                + "     pref:hasProperty3 'property3' .\n" //   3 seconds
                + "<urn:Fred>      a       pref:Person ; \n" //
                + "     pref:hasProperty4 'property4' ; \n" //
                + "     pref:hasProperty5 'property5' ; \n" //
                + "}";

        Update update = conn.prepareUpdate(QueryLanguage.SPARQL, sparqlInsert);
        update.execute();

        String queryString = "PREFIX pref: <http://www.model/pref#> \n" //
                + "SELECT ?x ?z \n" //
                + "WHERE { \n"
                + "  ?x a ?z. \n"
                + "  ?x pref:hasProperty1 'property1' . \n"//
                + "  ?x pref:hasProperty2 'property2' . \n"//
                + "  ?x pref:hasProperty3 'property3' . \n"//
                + "}";//



        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        CountingResultHandler tupleHandler = new CountingResultHandler();
        tupleQuery.evaluate(tupleHandler);
        log.info("Result count : " + tupleHandler.getCount());
        Validate.isTrue(tupleHandler.getCount() == 1);
        Validate.isTrue(tupleHandler.getBsSize() == 2);

        queryString = "PREFIX pref: <http://www.model/pref#> \n" //
                + "SELECT ?x ?w ?z \n" //
                + "WHERE { \n"
                + "  ?x a ?z. \n"
                + "  ?x pref:hasProperty4 'property4' . \n"//
                + "  ?x pref:hasProperty5 ?w . \n"//
                + "}";//


        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        tupleHandler = new CountingResultHandler();
        tupleQuery.evaluate(tupleHandler);
        log.info("Result count : " + tupleHandler.getCount());
        Validate.isTrue(tupleHandler.getCount() == 1);
        Validate.isTrue(tupleHandler.getBsSize() == 3);


        queryString = "PREFIX pref: <http://www.model/pref#> "
                + "SELECT ?v ?w ?x ?y ?z "
                + "WHERE { "
                + "  ?w a ?z  . "
                + "  ?w pref:hasProperty1 ?v . "
                + "  ?w pref:hasProperty2 'property2' . "
                + "  ?w pref:hasProperty3 'property3' . "
                + "  ?x a ?z  . "
                + "  ?x pref:hasProperty4 'property4' . "
                + "  ?x pref:hasProperty5 ?y . "
                + "}";



        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        tupleHandler = new CountingResultHandler();
        tupleQuery.evaluate(tupleHandler);
        log.info("Result count : " + tupleHandler.getCount());
        Validate.isTrue(tupleHandler.getCount() == 1);
        Validate.isTrue(tupleHandler.getBsSize() == 5);

    }


    private static Configuration getConf() {

        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, USE_MOCK_INSTANCE);
        conf.set(ConfigUtils.USE_ENTITY, "true");
        conf.setTablePrefix(RYA_TABLE_PREFIX);
        conf.set(ConfigUtils.CLOUDBASE_USER, "root");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, INSTANCE);
        conf.setInt(ConfigUtils.NUM_PARTITIONS, 3);
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, AUTHS);

        return conf;
    }


    private static class CountingResultHandler implements TupleQueryResultHandler {
        private int count = 0;
        private int bindingSize = 0;
        private boolean bsSizeSet = false;

        public int getCount() {
            return count;
        }

        public int getBsSize() {
            return bindingSize;
        }

        public void resetBsSize() {
            bindingSize = 0;
            bsSizeSet = false;
        }

        public void resetCount() {
            this.count = 0;
        }

        @Override
        public void startQueryResult(List<String> arg0) throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleSolution(BindingSet arg0) throws TupleQueryResultHandlerException {
            count++;
            if(!bsSizeSet) {
                bindingSize = arg0.size();
                bsSizeSet = true;
            }
            System.out.println(arg0);
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleBoolean(boolean arg0) throws QueryResultHandlerException {
        }

        @Override
        public void handleLinks(List<String> arg0) throws QueryResultHandlerException {
        }
    }
}
