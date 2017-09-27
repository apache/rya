package org.apache.rya.shell;


import java.io.IOException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration;
import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration.MongoDBIndexingConfigBuilder;
import org.apache.rya.mongodb.MongoConnectorFactory;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoTimeoutException;

public class MongoConnector {
	 private Configuration confx;
	 private SailRepository repository;
	 private SailRepositoryConnection conn;
	 private Sail sail;
	 
	 public MongoConnector() {
		 	confx=null;
	        repository = null;
	        conn = null;
	        sail=null;
	 }
	 
	 
	    public  Configuration getConf(String username, String password,String dbSpecs) throws IOException {
	        MongoDBIndexingConfigBuilder builder = MongoIndexingConfiguration.builder()
	                .setUseMockMongo(false).setUseInference(true).setAuths("U");
	        		
	                		//mongoURL:port:dbName:mPrefix
	                //String mongoURL, String mongoPort, String dbName, String mPrefix
	        		//mongoURL:mongoPort:dbName:mPrefix
	                String[] split_specs=dbSpecs.split(":");
	                if(split_specs.length!=4)
	                	return null;
	                if(username.length()<2 || password.length()<2)
	                	return null;
	               
	                String mongoURL=split_specs[0];
	                String mongoPort=split_specs[1];
	                String dbName=split_specs[2];
	                String mPrefix=split_specs[3];
	                builder = builder.setMongoUser(username)
	                        .setMongoPassword(password)
	                        .setMongoHost(mongoURL)
	                        .setMongoPort(mongoPort);
	               
	            return builder.setMongoDBName(dbName)
	                   .setMongoCollectionPrefix(mPrefix)
	                   .setUseMongoFreetextIndex(true)
	                   .setMongoFreeTextPredicates(RDFS.LABEL.stringValue()).build();

	    }
	    
	    public boolean establishConnection(String username, String password,String dbSpecs) {
	    	boolean connection_status=true;
	    	try {
				confx=getConf(username, password,dbSpecs);


	            sail = RyaSailFactory2.getInstance(confx);
	            repository = new SailRepository(sail);
	            conn = repository.getConnection();
	            //IOException
	            
			} catch (IOException e) {
				connection_status=false;
			}  catch (MongoSocketOpenException e)
	    	{
				connection_status=false;
	    	}
	    	catch (SailException e) {
				connection_status=false;
			} catch (AccumuloException e) {
				connection_status=false;
			} catch (AccumuloSecurityException e) {
				connection_status=false;
			} catch (RyaDAOException e) {
				connection_status=false;
			} catch (InferenceEngineException e) {
				connection_status=false;
			} 
	    	catch (RepositoryException e) {
				connection_status=false;
			}
	    	catch (MongoSecurityException e) {
	    		connection_status=false;
	    	}
	    	catch(MongoTimeoutException e) {
	    		connection_status=false;
	    	}
	    	


	    	return connection_status;
	    }
	    
	    public SailRepositoryConnection getConnector()
	    {
	    	return conn;
	    }
	    
	    public void closeAllConnection()
	    {
	        if (conn != null) {
	            try {
	                conn.close();
	            } catch (final RepositoryException e) {
	                // quietly absorb this exception
	            }
	        }
	    	
	        if (repository != null) {
	            try {
	                repository.shutDown();
	            } catch (final RepositoryException e) {
	                // quietly absorb this exception
	            }
	        }
	        MongoConnectorFactory.closeMongoClient();
	    }
	    
}
