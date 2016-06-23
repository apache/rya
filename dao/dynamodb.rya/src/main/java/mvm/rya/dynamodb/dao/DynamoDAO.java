package mvm.rya.dynamodb.dao;

import java.util.Iterator;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAO;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.RyaNamespaceManager;
import mvm.rya.api.persist.query.RyaQueryEngine;

public class DynamoDAO implements RyaDAO<DynamoRdfConfiguration> {
	
	private DynamoStorageStrategy strategy;
	private DynamoRdfConfiguration conf;
	private boolean isInitialized = false;

	@Override
	public void setConf(DynamoRdfConfiguration conf) {
		this.conf = conf;
	}

	@Override
	public DynamoRdfConfiguration getConf() {
		return conf;
	}

	@Override
	public void init() throws RyaDAOException {
		this.strategy = new DynamoStorageStrategy(conf);
		this.isInitialized = true;
	}

	@Override
	public boolean isInitialized() throws RyaDAOException {
		return isInitialized;
	}

	@Override
	public void destroy() throws RyaDAOException {
		strategy.close();
	}

	@Override
	public void add(RyaStatement statement) throws RyaDAOException {
		strategy.add(statement);
	}

	@Override
	public void add(Iterator<RyaStatement> statement) throws RyaDAOException {
		strategy.add(statement);
	}

	@Override
	public void delete(RyaStatement statement, DynamoRdfConfiguration conf) throws RyaDAOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dropGraph(DynamoRdfConfiguration conf, RyaURI... graphs) throws RyaDAOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(Iterator<RyaStatement> statements, DynamoRdfConfiguration conf) throws RyaDAOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getVersion() throws RyaDAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RyaQueryEngine<DynamoRdfConfiguration> getQueryEngine() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RyaNamespaceManager<DynamoRdfConfiguration> getNamespaceManager() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void purge(RdfCloudTripleStoreConfiguration configuration) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dropAndDestroy() throws RyaDAOException {
		// TODO Auto-generated method stub
		
	}

	public static void main(String[] args) throws Exception {
		  DynamoRdfConfiguration conf = new DynamoRdfConfiguration();
		  conf.setAWSUserName("FakeID");
		  conf.setAWSSecretKey("FakeKey");
		  conf.setAWSEndPoint("http://localhost:8000");
		  DynamoDAO dao = new DynamoDAO();
		  dao.setConf(conf);
		  dao.init();

	    }

}
