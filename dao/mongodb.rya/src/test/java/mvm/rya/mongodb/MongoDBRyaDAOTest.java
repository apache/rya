package mvm.rya.mongodb;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaStatement.RyaStatementBuilder;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;

public class MongoDBRyaDAOTest {
	
	private MongodForTestsFactory testsFactory;
	private MongoDBRyaDAO dao;
	private MongoDBRdfConfiguration configuration;
	private MongoClient mongoClient;
	
	@Before
	public void setUp() throws IOException, RyaDAOException{
		testsFactory = MongodForTestsFactory.with(Version.Main.PRODUCTION);
	       Configuration conf = new Configuration();
	        conf.set(MongoDBRdfConfiguration.USE_TEST_MONGO, "true");
	        conf.set(MongoDBRdfConfiguration.MONGO_DB_NAME, "test");
	        conf.set(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya_");
	        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
	        configuration = new MongoDBRdfConfiguration(conf);
			mongoClient = testsFactory.newMongo();
            int port = mongoClient.getServerAddressList().get(0).getPort();
            configuration.set(MongoDBRdfConfiguration.MONGO_INSTANCE_PORT, Integer.toString(port));
			dao = new MongoDBRyaDAO(configuration, mongoClient);
		
	}

	@Test
	public void testDeleteWildcard() throws RyaDAOException {
		RyaStatementBuilder builder = new RyaStatementBuilder();
		builder.setPredicate(new RyaURI("http://temp.com"));
		dao.delete(builder.build(), configuration);
	}
	
	
	@Test
	public void testAdd() throws RyaDAOException, MongoException, IOException {
		RyaStatementBuilder builder = new RyaStatementBuilder();
		builder.setPredicate(new RyaURI("http://temp.com"));
		builder.setSubject(new RyaURI("http://subject.com"));
		builder.setObject(new RyaURI("http://object.com"));
		
		DB db = mongoClient.getDB(configuration.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
        DBCollection coll = db.getCollection(configuration.getTriplesCollectionName());
          
		dao.add(builder.build());

        assertEquals(coll.count(),1);
		
	}
	
	@Test
	public void testDelete() throws RyaDAOException, MongoException, IOException {
		RyaStatementBuilder builder = new RyaStatementBuilder();
		builder.setPredicate(new RyaURI("http://temp.com"));
		builder.setSubject(new RyaURI("http://subject.com"));
		builder.setObject(new RyaURI("http://object.com"));
		RyaStatement statement = builder.build();
		
		DB db = mongoClient.getDB(configuration.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
        DBCollection coll = db.getCollection(configuration.getTriplesCollectionName());
          
		dao.add(statement);

        assertEquals(coll.count(),1);
		
        dao.delete(statement, configuration);
        
        assertEquals(coll.count(),0);

	}

	@Test
	public void testDeleteWildcardSubjectWithContext() throws RyaDAOException, MongoException, IOException {
		RyaStatementBuilder builder = new RyaStatementBuilder();
		builder.setPredicate(new RyaURI("http://temp.com"));
		builder.setSubject(new RyaURI("http://subject.com"));
		builder.setObject(new RyaURI("http://object.com"));
		builder.setContext(new RyaURI("http://context.com"));
		RyaStatement statement = builder.build();
		
		DB db = mongoClient.getDB(configuration.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
        DBCollection coll = db.getCollection(configuration.getTriplesCollectionName());
          
		dao.add(statement);

        assertEquals(coll.count(),1);
        
		RyaStatementBuilder builder2 = new RyaStatementBuilder();
		builder2.setPredicate(new RyaURI("http://temp.com"));
		builder2.setObject(new RyaURI("http://object.com"));
		builder2.setContext(new RyaURI("http://context3.com"));
		RyaStatement query = builder2.build();
		
        dao.delete(query, configuration);
        
        assertEquals(coll.count(),1);

	}

}
