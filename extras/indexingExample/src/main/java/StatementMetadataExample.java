
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import org.apache.accumulo.core.client.Connector;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.statement.metadata.matching.StatementMetadataOptimizer;
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

public class StatementMetadataExample {

    private static final Logger log = Logger.getLogger(StatementMetadataExample.class);
    private AccumuloRyaDAO dao;
    private Sail sail;
    private SailRepository repository;
    private SailRepositoryConnection conn;

    public StatementMetadataExample(AccumuloRdfConfiguration conf) throws Exception {
        Connector aConn = ConfigUtils.getConnector(conf);
        dao = new AccumuloRyaDAO();
        dao.setConnector(aConn);
        dao.init();

        sail = RyaSailFactory.getInstance(conf);
        repository = new SailRepository(sail);
        conn = repository.getConnection();
    }

    public static void main(final String[] args) throws Exception {
        setLogger();
        
        StatementMetadataExample example = new StatementMetadataExample(getConf());
        example.example1();
        example.example2();
        example.example3();
        example.example4();
        example.close();

    }
    
    /**
     * This example demonstrates how a reified query can be used to return
     * metadata. In the example below, the query returns all the places that Joe
     * works at along with the people that created the triples containing those locations
     * and the dates that those triples were created on. 
     * 
     * @throws Exception
     */
    private void example1() throws Exception {

        String query1 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX owl: <http://www.w3.org/2002/07/owl#> \n"
                + "SELECT ?x ?y ?z \n" 
                + "WHERE {\n"
                + "_:blankNode rdf:type owl:Annotation. \n" 
                + "_:blankNode owl:annotatedSource <http://Joe>. \n"
                + "_:blankNode owl:annotatedProperty <http://worksAt>. \n" 
                + "_:blankNode owl:annotatedTarget ?x. \n"
                + "_:blankNode <http://createdBy> ?y. \n" 
                + "_:blankNode <http://createdOn> ?z }\n";

        StatementMetadata metadata1 = new StatementMetadata();
        metadata1.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Dave"));
        metadata1.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-02"));

        RyaStatement statement1 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaURI("http://context"), "", metadata1);

        StatementMetadata metadata2 = new StatementMetadata();
        metadata2.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Dave"));
        metadata2.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-02-04"));

        RyaStatement statement2 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("HardwareStore"), new RyaURI("http://context"), "", metadata2);

        StatementMetadata metadata3 = new StatementMetadata();
        metadata3.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Fred"));
        metadata3.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-03-08"));

        RyaStatement statement3 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("Library"), new RyaURI("http://context"), "", metadata3);

        // add statements for querying
        dao.add(statement1);
        dao.add(statement2);
        dao.add(statement3);

        System.out.println("**************************************************************************");
        System.out.println("                            RUNNING EXAMPLE 1");
        System.out.println("**************************************************************************");
        System.out.println("");
        
        // issue query - 3 results expected
        query(query1, 3);

        // delete statements to run next example
        dao.delete(Arrays.asList(statement1, statement2, statement3).iterator(), getConf());
    }

    /**
     * This example demonstrates how a reified query can be used to return
     * metadata and that a query can contain a mix of reified metadata triple
     * patterns and non-reified triple patterns. In the query below, all patterns
     * containing _:blankNode in the subject will be grouped into a 
     * {@link StatementMetadataNode}, so the resulting query plan will consist
     * of this metadata node joined along the variable ?x 
     * with the non-reified pattern ?x <http://locatedWithin> <http://UnitedState>.
     * In the example below, the query returns all locations that Doug
     * travels to in the UnitedStates and the timeStamp of the triples containing
     * that information.  This example further demonstrates that any number 
     * of registered metadata properties can be queried using the contextual metadata index.
     * 
     * @throws Exception
     */
    private void example2() throws Exception {

        String query2 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX owl: <http://www.w3.org/2002/07/owl#> \n" 
                + "SELECT ?x ?y \n" + "WHERE {\n"
                + "_:blankNode rdf:type owl:Annotation. \n" 
                + "_:blankNode owl:annotatedSource <http://Doug>. \n"
                + "_:blankNode owl:annotatedProperty <http://travelsTo>. \n" 
                + "_:blankNode owl:annotatedTarget ?x. \n"
                + "?x <http://locatedWithin> <http://UnitedStates> . \n" 
                + "_:blankNode <http://hasTimeStamp> ?y }\n";

        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaURI("http://hasTimeStamp"), new RyaType(XMLSchema.TIME, "09:30:10.5"));

        RyaStatement statement1 = new RyaStatement(new RyaURI("http://Doug"), new RyaURI("http://travelsTo"),
                new RyaURI("http://NewMexico"), new RyaURI("http://context"), "", metadata);
        RyaStatement statement2 = new RyaStatement(new RyaURI("http://NewMexico"), new RyaURI("http://locatedWithin"),
                new RyaType("http://UnitedStates"), new RyaURI("http://context"), "", new StatementMetadata());

        // add statements for querying
        dao.add(statement1);
        dao.add(statement2);

        System.out.println("**************************************************************************");
        System.out.println("                            RUNNING EXAMPLE 2");
        System.out.println("**************************************************************************");
        System.out.println("");
        
        // issue query - 1 results expected
        query(query2, 1);
        
        // delete statements to run next example
        dao.delete(Arrays.asList(statement1, statement2).iterator(), getConf());
    }

    /**
     * In addition to returning metadata, this example demonstrates how a
     * reified query can be used to return only those triples matching certain
     * metadata criteria.  The query below returns only those triples containing Joe's
     * work location that were created on 2017-02-04.  To filter by metadata property, simply set the
     * value for the property to a fixed constant ('2017-02-04'^^xsd:date is set for the property
     * http://createdOn in the query below).
     * 
     * @throws Exception
     */
    private void example3() throws Exception {

        String query3 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX owl: <http://www.w3.org/2002/07/owl#> \n" 
                + "SELECT ?x ?y \n" 
                + "WHERE {\n"
                + "_:blankNode rdf:type owl:Annotation. \n" 
                + "_:blankNode owl:annotatedSource <http://Joe>. \n"
                + "_:blankNode owl:annotatedProperty <http://worksAt>. \n" 
                + "_:blankNode owl:annotatedTarget ?x. \n"
                + "_:blankNode <http://createdBy> ?y. \n" 
                + "_:blankNode <http://createdOn> '2017-02-04'^^xsd:date }\n";

        StatementMetadata metadata1 = new StatementMetadata();
        metadata1.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Dave"));
        metadata1.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-02"));

        RyaStatement statement1 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaURI("http://context"), "", metadata1);

        StatementMetadata metadata2 = new StatementMetadata();
        metadata2.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Dave"));
        metadata2.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-02-04"));

        RyaStatement statement2 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("HardwareStore"), new RyaURI("http://context"), "", metadata2);

        StatementMetadata metadata3 = new StatementMetadata();
        metadata3.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Fred"));
        metadata3.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-03-08"));

        RyaStatement statement3 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("Library"), new RyaURI("http://context"), "", metadata3);

        // add statements for querying
        dao.add(statement1);
        dao.add(statement2);
        dao.add(statement3);

        System.out.println("**************************************************************************");
        System.out.println("                            RUNNING EXAMPLE 3");
        System.out.println("**************************************************************************");
        System.out.println("");
        
        // issue query - 1 result expected
        query(query3, 1);

        // delete statements to run next example
        dao.delete(Arrays.asList(statement1, statement2, statement3).iterator(), getConf());
    }

    /**
     * 
     * This example demonstrates the ability to join across two differnt StatementMetadataNodes.
     * The query below consists of triple patterns with _:blankNode1 as the subject and patterns
     * with _:blankNode2 as the subject.  Each of these groups of patterns will be used to form
     * a @link{StatementMetadataNode} to efficiently query for the underlying triples and metadata.
     * The effective query plan of the following query will consist of two StatementMetaData nodes
     * that will be joined on the variables ?x and ?y -- the values of owl:annotatedTarget and the
     * metadata property http://createdBy.  So the query below will return the work locations that Joe 
     * and Bob have in common that appear in triples created by the same person, in addition to the dates
     * that each triple was created on.   
     * 
     * @throws Exception
     */
    private void example4() throws Exception {

         String query4 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX owl: <http://www.w3.org/2002/07/owl#> \n" 
                + "SELECT ?x ?y ?z ?a \n" + "WHERE {\n"
                + "_:blankNode1 rdf:type owl:Annotation. \n" 
                + "_:blankNode1 owl:annotatedSource <http://Joe>. \n"
                + "_:blankNode1 owl:annotatedProperty <http://worksAt>. \n" 
                + "_:blankNode1 owl:annotatedTarget ?x. \n"
                + "_:blankNode1 <http://createdBy> ?y. \n" 
                + "_:blankNode1 <http://createdOn> ?z. \n"
                + "_:blankNode2 rdf:type owl:Annotation. \n" 
                + "_:blankNode2 owl:annotatedSource <http://Bob>. \n"
                + "_:blankNode2 owl:annotatedProperty <http://worksAt>. \n" 
                + "_:blankNode2 owl:annotatedTarget ?x. \n"
                + "_:blankNode2 <http://createdBy> ?y. \n" 
                + "_:blankNode2 <http://createdOn> ?a }\n";

        StatementMetadata metadata1 = new StatementMetadata();
        metadata1.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Dave"));
        metadata1.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-02"));

        RyaStatement statement1 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaURI("http://context"), "", metadata1);

        StatementMetadata metadata2 = new StatementMetadata();
        metadata2.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Dave"));
        metadata2.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-02-04"));

        RyaStatement statement2 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("HardwareStore"), new RyaURI("http://context"), "", metadata2);

        StatementMetadata metadata3 = new StatementMetadata();
        metadata3.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Fred"));
        metadata3.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-03-08"));

        RyaStatement statement3 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("Library"), new RyaURI("http://context"), "", metadata3);
        
        StatementMetadata metadata4 = new StatementMetadata();
        metadata4.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Dave"));
        metadata4.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-04-16"));

        RyaStatement statement4 = new RyaStatement(new RyaURI("http://Bob"), new RyaURI("http://worksAt"),
                new RyaType("HardwareStore"), new RyaURI("http://context"), "", metadata4);

        // add statements for querying
        dao.add(statement1);
        dao.add(statement2);
        dao.add(statement3);
        dao.add(statement4);

        System.out.println("**************************************************************************");
        System.out.println("                            RUNNING EXAMPLE 4");
        System.out.println("**************************************************************************");
        System.out.println("");
        
        // issue query - 1 result expected
        query(query4, 1);

        // delete statements to run next example
        dao.delete(Arrays.asList(statement1, statement2, statement3, statement4).iterator(), getConf());
    }
    

    public void query(String query, int expected) throws Exception {
        prettyPrintQuery(query);
        prettyPrintQueryPlan(query);
        CountingResultHandler resultHandler = new CountingResultHandler();
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.evaluate(resultHandler);
        Validate.isTrue(expected == resultHandler.getCount());
    }

    private static AccumuloRdfConfiguration getConf() {

        AccumuloRdfConfiguration conf;
        Set<RyaURI> propertySet = new HashSet<RyaURI>(Arrays.asList(new RyaURI("http://createdBy"),
                new RyaURI("http://createdOn"), new RyaURI("http://hasTimeStamp")));
        conf = new AccumuloRdfConfiguration();
        conf.setDisplayQueryPlan(false);
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
        conf.set(ConfigUtils.CLOUDBASE_USER, "root");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "instance");
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");
        conf.setUseStatementMetadata(true);
        conf.setStatementMetadataProperties(propertySet);
        return conf;
    }

    public void close() {
        try {
            System.out.println("Closing SailRepositoryConnection");
            conn.close();
            System.out.println("Shutting down SailRepository");
            repository.shutDown();
            System.out.println("Shutting down Sail.");
            sail.shutDown();
            dao.destroy();
        } catch (RyaDAOException | RepositoryException | SailException e) {
            System.out.println("Unable to cleanly shut down all resources.");
            e.printStackTrace();
            System.exit(0);
        }
    }

    private void prettyPrintQuery(String query) {
        System.out.println("=================== SPARQL QUERY ============================");

        for (String str : query.split("\\r?\\n")) {
            System.out.println(str);
        }

        System.out.println("=================== END SPARQL QUERY ========================");
        System.out.println("");

    }
    
    private void prettyPrintQueryPlan(String query) throws MalformedQueryException {
        
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        TupleExpr exp = pq.getTupleExpr();
        new StatementMetadataOptimizer(getConf()).optimize(exp,null,null);
        
        System.out.println("=================== RYA SPARQL QUERY PLAN ===================");
        for (String str : exp.toString().split("\\r?\\n")) {
            System.out.println(str);
        }
        System.out.println("=================== END RYA QUERY PLAN ======================");
        
    }
    
    private static void setLogger() {
        Logger.getLogger(Configuration.class).setLevel(Level.ERROR);
        Logger.getLogger(NativeCodeLoader.class).setLevel(Level.ERROR);
        Logger.getLogger(RyaSailFactory.class).setLevel(Level.ERROR);
    }

    private static class CountingResultHandler implements TupleQueryResultHandler {

        private int count = 0;

        public int getCount() {
            return count;
        }

        @Override
        public void startQueryResult(final List<String> arg0) throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleSolution(final BindingSet arg0) throws TupleQueryResultHandlerException {
            if (count == 0) {
                System.out.println("");
                System.out.println("==================== QUERY RESULTS ==========================");
            }
            System.out.println(arg0);
            count++;
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
            System.out.println("==================== END QUERY RESULTS ======================");
            System.out.println("");
            System.out.println("");
            System.out.println("");

        }

        @Override
        public void handleBoolean(final boolean arg0) throws QueryResultHandlerException {
        }

        @Override
        public void handleLinks(final List<String> arg0) throws QueryResultHandlerException {
        }
    }
}
