package mvm.rya.mongodb.iter;

import java.util.Iterator;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;

public class NonCloseableRyaStatementCursorIterator implements Iterator<RyaStatement> {

	RyaStatementCursorIterator iterator;
	
	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public RyaStatement next() {
		return iterator.next();
	}

	public NonCloseableRyaStatementCursorIterator(
			RyaStatementCursorIterator iterator) {
		this.iterator = iterator;
	}

	@Override
	public void remove() {
		try {
			iterator.remove();
		} catch (RyaDAOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
