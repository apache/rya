package mvm.rya.mongodb.iter;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.mongodb.dao.MongoDBStorageStrategy;

import org.calrissian.mango.collect.CloseableIterable;
import org.openrdf.query.BindingSet;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class RyaStatementCursorIterator implements CloseableIteration<RyaStatement, RyaDAOException> {

	private DBCollection coll;
	private Iterator<DBObject> queryIterator;
	private DBCursor currentCursor;
	private MongoDBStorageStrategy strategy;
	private Long maxResults;

	public RyaStatementCursorIterator(DBCollection coll, Set<DBObject> queries, MongoDBStorageStrategy strategy) {
		this.coll = coll;
		this.queryIterator = queries.iterator();
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
	public RyaStatement next() {
		if (!currentCursorIsValid()) {
			findNextValidCursor();
		}
		if (currentCursorIsValid()) {
			// convert to Rya Statement
			DBObject queryResult = currentCursor.next();
			RyaStatement statement = strategy.deserializeDBObject(queryResult);
			return statement;
		}
		return null;
	}
	
	private void findNextValidCursor() {
		while (queryIterator.hasNext()){
			DBObject currentQuery = queryIterator.next();
			currentCursor = coll.find(currentQuery);
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
