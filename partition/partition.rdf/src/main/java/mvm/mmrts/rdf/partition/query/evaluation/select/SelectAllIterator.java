package mvm.mmrts.rdf.partition.query.evaluation.select;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import com.google.common.collect.Lists;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Var;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Class SelectAllIterator
 * Date: Jul 18, 2011
 * Time: 12:01:25 PM
 */
public class SelectAllIterator extends SelectIterator {

    private List<Map.Entry<Var, Var>> predObj;
    private List<Statement> document = null;
    private int index = 0;

    public SelectAllIterator(BindingSet bindings, Iterator<Map.Entry<Key, Value>> iter, Var predVar, Var objVar) throws QueryEvaluationException {
        super(bindings, iter);
        predObj = (List) Lists.newArrayList(new HashMap.SimpleEntry(predVar, objVar));
    }

    @Override
    public boolean hasNext() throws QueryEvaluationException {
        return super.hasNext() || document != null;
    }

    @Override
    public BindingSet next() throws QueryEvaluationException {
        try {
            if (document == null && super.hasNext()) {
                document = nextDocument();
            }
            Statement st = document.get(index);
            index++;
            if (index >= document.size()) {
                document = null;
            }
            return populateBindingSet(st, predObj);
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

}
