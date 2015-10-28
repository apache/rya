package mvm.rya.mongodb.iter;

import info.aduna.iteration.CloseableIteration;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;

import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterator;
import org.openrdf.query.BindingSet;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class RyaStatementCursorIterable implements CloseableIterable<RyaStatement> {


	private NonCloseableRyaStatementCursorIterator iterator;

	public RyaStatementCursorIterable(NonCloseableRyaStatementCursorIterator iterator) {
		this.iterator = iterator;
	}

	@Override
	public Iterator<RyaStatement> iterator() {
		// TODO Auto-generated method stub
		return iterator;
	}

	@Override
	public void closeQuietly() {
		//TODO  don't know what to do here
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

}
