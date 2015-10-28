package mvm.rya.indexing;

import info.aduna.iteration.CloseableIteration;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet.AccValueFactory;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;



public interface PrecompQueryIndexer extends Closeable, Flushable {

    
    public abstract void storeBindingSet(BindingSet bs) throws IOException ;

    public abstract void storeBindingSets(Collection<BindingSet> bindingSets)
            throws IOException, IllegalArgumentException;


    public abstract CloseableIteration<BindingSet, QueryEvaluationException> queryPrecompJoin(List<String> varOrder, 
            String localityGroup, Map<String, AccValueFactory> bindings, Map<String,Value> valMap, 
            Collection<BindingSet> constraints) throws QueryEvaluationException,TableNotFoundException;

   

    @Override
    public abstract void flush() throws IOException;

    @Override
    public abstract void close() throws IOException;
}

 