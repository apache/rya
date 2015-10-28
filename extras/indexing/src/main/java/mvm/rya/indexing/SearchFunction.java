package mvm.rya.indexing;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.Statement;
import org.openrdf.query.QueryEvaluationException;

/**
 * A function used to perform a search.
 */
public interface SearchFunction {

    /**
     * Search the indices for the given terms and return {@link Statement}s that meet the {@link StatementContraints}
     * 
     * @param searchTerms
     *            the search terms
     * @param contraints
     *            the constraints on the returned {@link Statement}s
     * @return
     * @throws QueryEvaluationException
     */
    public abstract CloseableIteration<Statement, QueryEvaluationException> performSearch(String searchTerms, StatementContraints contraints)
            throws QueryEvaluationException;

}