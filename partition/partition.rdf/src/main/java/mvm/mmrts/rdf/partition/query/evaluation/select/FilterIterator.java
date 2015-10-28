package mvm.mmrts.rdf.partition.query.evaluation.select;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import com.google.common.collect.Lists;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import java.util.*;

/**
 * TODO: This could be done as a filtering iterator in the Iterator Stack
 */
public class FilterIterator extends SelectIterator {

    private List<Map.Entry<Var, Var>> predObjs;
    private Map<URI, Map.Entry<Var, Var>> filters = new HashMap<URI, Map.Entry<Var, Var>>();
    private List<Statement> document;
    private List<Map.Entry<Var, Var>> currentPredObj;
    private Var subjVar;
    private List<QueryBindingSet> currentResults;
    private int currentResultsIndex = 0;

    public FilterIterator(BindingSet bindings, Iterator<Map.Entry<Key, Value>> iter, Var subjVar, List<Map.Entry<Var, Var>> predObjs) throws QueryEvaluationException {
        super(bindings, iter);
        this.subjVar = subjVar;
        this.predObjs = predObjs;
        for (Map.Entry<Var, Var> predObj : this.predObjs) {
            //find filtering predicates
            this.filters.put((URI) predObj.getKey().getValue(), predObj);
        }
    }

    @Override
    public boolean hasNext() throws QueryEvaluationException {
        if (document != null || currentResults != null)
            return true;

        return super.hasNext();

//        boolean hasNext = super.hasNext();
//        List<Map.Entry<Var, Var>> filter = null;
//        while (hasNext) {
//            List<Statement> stmts = nextDocument();
//            filter = filter(stmts);
//            if (filter != null && filter.size() > 0) {
//                document = stmts;
//                this.currentPredObj = filter;
//                return true;
//            }
//            hasNext = super.hasNext();
//        }
//        return document != null;
    }

    @Override
    public BindingSet next() throws QueryEvaluationException {
        try {
            if (document == null) {
                document = nextDocument();
            }
            if (currentResults == null) {
                currentResults = populateBindingSet(document, subjVar, this.predObjs);
            }
            BindingSet bs = currentResults.get(currentResultsIndex);
            currentResultsIndex++;
            if (currentResultsIndex >= currentResults.size()) {
                currentResults = null;
                currentResultsIndex = 0;
                document = null;
            }
            return bs;
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    /**
     * @return true if the Statement is filtered
     * @throws QueryEvaluationException
     */
    protected List<Map.Entry<Var, Var>> filter(List<Statement> document) throws QueryEvaluationException {
        List<Map.Entry<Var, Var>> foundIn = new ArrayList();

        for (Statement st : document) {
            for (Map.Entry<Var, Var> entry : this.predObjs) {
                if (st.getPredicate().equals(entry.getKey().getValue())) {
                    foundIn.add(entry);
                    break;
                }
            }
        }
        return foundIn;
    }

}
