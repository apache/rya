package mvm.rya.dynamodb.dao;

import java.util.Collection;
import java.util.Map.Entry;

import org.calrissian.mango.collect.CloseableIterable;
import org.openrdf.query.BindingSet;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.query.BatchRyaQuery;
import mvm.rya.api.persist.query.RyaQuery;
import mvm.rya.api.persist.query.RyaQueryEngine;

public class DynamoQueryEngine implements RyaQueryEngine<DynamoRdfConfiguration> {
	
	
	private DynamoRdfConfiguration conf;
	private DynamoStorageStrategy storageStrategy;
	
	

	@Override
	public void setConf(DynamoRdfConfiguration conf) {
		this.conf = conf;
	}
	
	public void setStorageStrategy(DynamoStorageStrategy strategy){
		this.storageStrategy = strategy;
	}

	@Override
	public DynamoRdfConfiguration getConf() {
		return conf;
	}

	@Override
	public CloseableIteration<RyaStatement, RyaDAOException> query(RyaStatement stmt, DynamoRdfConfiguration conf)
			throws RyaDAOException {
		if (conf == null){
			this.conf = conf;
		}
		
		return null;
	}

	@Override
	public CloseableIteration<? extends Entry<RyaStatement, BindingSet>, RyaDAOException> queryWithBindingSet(
			Collection<Entry<RyaStatement, BindingSet>> stmts, DynamoRdfConfiguration conf) throws RyaDAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIteration<RyaStatement, RyaDAOException> batchQuery(Collection<RyaStatement> stmts,
			DynamoRdfConfiguration conf) throws RyaDAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIterable<RyaStatement> query(RyaQuery ryaQuery) throws RyaDAOException {
		return null;
	}

	@Override
	public CloseableIterable<RyaStatement> query(BatchRyaQuery batchRyaQuery) throws RyaDAOException {
		// TODO Auto-generated method stub
		return null;
	}
}
