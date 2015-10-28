package mvm.rya.mongodb.iter;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.mongodb.dao.MongoDBStorageStrategy;

import org.openrdf.query.BindingSet;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class RyaStatementBindingSetCursorIterator implements CloseableIteration<Entry<RyaStatement, BindingSet>, RyaDAOException> {

	private DBCollection coll;
	private Map<DBObject, BindingSet> rangeMap;
	private Iterator<DBObject> queryIterator;
	private Long maxResults;
	private DBCursor currentCursor;
	private BindingSet currentBindingSet;
	private MongoDBStorageStrategy strategy;

	public RyaStatementBindingSetCursorIterator(DBCollection coll,
			Map<DBObject, BindingSet> rangeMap, MongoDBStorageStrategy strategy) {
		this.coll = coll;
		this.rangeMap = rangeMap;
		this.queryIterator = rangeMap.keySet().iterator();
		this.strategy = strategy;
	}

	@Override
	public boolean hasNext() {
		if (!currentCursorIsValid()) {
			findNextValidCursor();
		}
		return currentCursorIsValid();
	}

	@Override
	public Entry<RyaStatement, BindingSet> next() {
		if (!currentCursorIsValid()) {
			findNextValidCursor();
		}
		if (currentCursorIsValid()) {
			// convert to Rya Statement
			DBObject queryResult = currentCursor.next();
			RyaStatement statement = strategy.deserializeDBObject(queryResult);
			return new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(statement, currentBindingSet);
		}
		return null;
	}
	
	private void findNextValidCursor() {
		while (queryIterator.hasNext()){
			DBObject currentQuery = queryIterator.next();
			currentCursor = coll.find(currentQuery);
			currentBindingSet = rangeMap.get(currentQuery);
			if (currentCursor.hasNext()) break;
		}
	}
	
	private boolean currentCursorIsValid() {
		return (currentCursor != null) && currentCursor.hasNext();
	}


	public void setMaxResults(Long maxResults) {
		this.maxResults = maxResults;
	}

	@Override
	public void close() throws RyaDAOException {
		// TODO don't know what to do here
	}

	@Override
	public void remove() throws RyaDAOException {
		next();
	}

}
