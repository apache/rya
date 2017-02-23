
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
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

public class StatementMetadataExample {
    
    private static final Logger log = Logger.getLogger(StatementMetadataExample.class);
    // show discoverability of metadata
    private static final String query1 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
            + "prefix owl: <http://www.w3.org/2002/07/owl#> \n"
            + "SELECT ?x ?y ?z \n"
            + "WHERE {\n"
            + "_:blankNode rdf:type owl:Annotation. \n"
            + "_:blankNode owl:annotatedSource <http://Joe>. \n"
            + "_:blankNode owl:annotatedProperty <http://worksAt>. \n"
            + "_:blankNode owl:annotatedTarget ?x. \n" 
            + "_:blankNode <http://createdBy> ?y. \n"
            + "_:blankNode <http://createdOn> ?z }\n";
    // show that metadata can be used as a search criteria
    private static final String query2 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
            + "prefix owl: <http://www.w3.org/2002/07/owl#> \n"
            + "SELECT ?x ?y \n"
            + "WHERE {\n"
            + "_:blankNode rdf:type owl:Annotation. \n"
            + "_:blankNode owl:annotatedSource <http://Joe>. \n"
            + "_:blankNode owl:annotatedProperty <http://worksAt>. \n"
            + "_:blankNode owl:annotatedTarget ?x. \n" 
            + "_:blankNode <http://createdBy> ?y. \n"
            + "_:blankNode <http://createdOn> '2017-02-04'^^xsd:date }\n";
    // join across metadata
    private static final String query3 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
            + "prefix owl: <http://www.w3.org/2002/07/owl#> \n"
            + "SELECT ?x ?y ?z ?a \n"
            + "WHERE {\n"
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
    private static final String query4 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
            + "prefix owl: <http://www.w3.org/2002/07/owl#> \n"
            + "SELECT ?x ?y \n"
            + "WHERE {\n"
            + "_:blankNode rdf:type owl:Annotation. \n"
            + "_:blankNode owl:annotatedSource <http://Doug>. \n"
            + "_:blankNode owl:annotatedProperty <http://travelsTo>. \n"
            + "_:blankNode owl:annotatedTarget ?x. \n" 
            + "?x <http://locatedWithin> <http://UnitedStates> . \n" 
            + "_:blankNode <http://hasTimeStamp> ?y }\n";
  
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
       
        StatementMetadataExample example = new StatementMetadataExample(getConf());
        example.populateWithData();
        example.query(query1,3);
        example.query(query2,1);
        example.query(query3,1);
        example.query(query4,1);
        
        example.close();

    }

    private void populateWithData() throws RyaDAOException {
        
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
        
        StatementMetadata metadata5 = new StatementMetadata();
        metadata5.addMetadata(new RyaURI("http://hasTimeStamp"), new RyaType(XMLSchema.TIME, "09:30:10.5"));
        
        RyaStatement statement5 = new RyaStatement(new RyaURI("http://Doug"), new RyaURI("http://travelsTo"),
                new RyaURI("http://NewMexico"), new RyaURI("http://context"), "", metadata5);
        RyaStatement statement6 = new RyaStatement(new RyaURI("http://NewMexico"), new RyaURI("http://locatedWithin"),
                new RyaType("http://UnitedStates"), new RyaURI("http://context"), "", new StatementMetadata());
        
        dao.add(statement1);
        dao.add(statement2);
        dao.add(statement3);
        dao.add(statement4);
        dao.add(statement5);
        dao.add(statement6);
    }
    
    public void query(String query, int expected) throws Exception {
        prettyPrintQuery(query);
        CountingResultHandler resultHandler = new CountingResultHandler();
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.evaluate(resultHandler);
        Validate.isTrue(expected == resultHandler.getCount());
    }
    
    private static AccumuloRdfConfiguration getConf() {

        AccumuloRdfConfiguration conf;
        Set<RyaURI> propertySet = new HashSet<RyaURI>(
                Arrays.asList(new RyaURI("http://createdBy"), new RyaURI("http://createdOn"), new RyaURI("http://hasTimeStamp")));
        conf = new AccumuloRdfConfiguration();
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
            log.info("Closing SailRepositoryConnection");
            conn.close();
            log.info("Shutting down SailRepository");
            repository.shutDown();
            log.info("Shutting down Sail.");
            sail.shutDown();
            dao.destroy();
        } catch (RyaDAOException | RepositoryException | SailException e) {
            log.info("Unable to cleanly shut down all resources.");
            e.printStackTrace();
            System.exit(0);
        }
    }

    private void prettyPrintQuery(String query) {
        log.info("=================== SPARQL Query ===================");

        for (String str : query.split("\\r?\\n")) {
            log.info(str);
        }
        
        log.info("=================== END SPARQL Query ===================");

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
            if(count == 0) {
                System.out.println("");
                log.info("=================== Query Results ===================");
            }
            log.info(arg0);
            count++;
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
            log.info("=================== End Query Results ===================");
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
