package mvm.rya.dynamodb;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;

import com.almworks.sqlite4java.SQLite;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.dynamodb.dao.DynamoDAO;
import mvm.rya.dynamodb.dao.DynamoRdfConfiguration;

public class DynamoDAOTest {
	
	private int getCount(CloseableIteration<RyaStatement, RyaDAOException> queryResults) throws RyaDAOException{
		int count = 0;
		  while(queryResults.hasNext()){
			  System.out.println(queryResults.next());
			  count++;
		  }
		  return count;
	}

	@Test
	public void testAddAndQuery() throws RyaDAOException {
		  DynamoRdfConfiguration conf = new DynamoRdfConfiguration();
		  conf.setTablePrefix("rya");
		  conf.setBoolean(DynamoRdfConfiguration.USE_MOCK, true);
		  SQLite.setLibraryPath("target/lib");
		  System.out.println("Start.....");
		  DynamoDAO dao = new DynamoDAO();
		  
		  dao.setConf(conf);
		  AmazonDynamoDB client = DynamoDBUtils.getDynamoDBClientFromConf(conf);
		  dao.setDynamoDB(client);
		  dao.init();
		  System.out.println("Done initializing..");
		  RyaStatement statement = new RyaStatement(new RyaURI("urn:subj"), new RyaURI("urn:pred"), new RyaURI("urn:obj"));
		  RyaStatement statement2 = new RyaStatement(new RyaURI("urn:subj"), new RyaURI("urn:pred2"), new RyaURI("urn:obj"));
		  RyaStatement subjQuery = new RyaStatement(new RyaURI("urn:subj"), null, new RyaURI("urn:obj"));
		  RyaStatement predQuery = new RyaStatement(null, new RyaURI("urn:pred"), null);
		  RyaStatement objQuery = new RyaStatement(null, null, new RyaURI("urn:obj"));
		  RyaStatement poQuery = new RyaStatement(null, new RyaURI("urn:pred"), new RyaURI("urn:obj"));
		  RyaStatement spQuery = new RyaStatement(new RyaURI("urn:subj"), new RyaURI("urn:pred"), null);
		  System.out.println("Done initializing..");
		  dao.add(statement);
		  System.out.println("Done initializing..");
		  dao.add(statement2);
		  CloseableIteration<RyaStatement, RyaDAOException> queryResults = dao.getQueryEngine().query(subjQuery, conf);		  
		  assertEquals(getCount(queryResults), 2);
		  System.out.println("Start.....");

		  queryResults = dao.getQueryEngine().query(predQuery, conf);		  
		  assertEquals(getCount(queryResults), 1);
		  System.out.println("Start.....");

		  queryResults = dao.getQueryEngine().query(objQuery, conf);		  
		  assertEquals(getCount(queryResults), 2);
		  System.out.println("Start.....");

		  queryResults = dao.getQueryEngine().query(poQuery, conf);		  
		  assertEquals(getCount(queryResults), 1);
		  System.out.println("Start.....");

		  queryResults = dao.getQueryEngine().query(spQuery, conf);		  
		  assertEquals(getCount(queryResults), 1);
		  System.out.println("Start.....");
		  
		  dao.destroy();
	    }

}
