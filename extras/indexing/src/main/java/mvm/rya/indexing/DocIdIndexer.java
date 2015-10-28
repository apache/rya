package mvm.rya.indexing;

import info.aduna.iteration.CloseableIteration;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import mvm.rya.indexing.accumulo.entity.StarQuery;
import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet.AccValueFactory;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

public interface DocIdIndexer extends Closeable {

  

    public abstract CloseableIteration<BindingSet, QueryEvaluationException> queryDocIndex(StarQuery query,
            Collection<BindingSet> constraints) throws TableNotFoundException, QueryEvaluationException;

   

    @Override
    public abstract void close() throws IOException;
    
}
