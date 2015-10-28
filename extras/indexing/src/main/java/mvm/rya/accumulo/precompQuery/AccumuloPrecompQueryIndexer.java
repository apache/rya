package mvm.rya.accumulo.precompQuery;

import info.aduna.iteration.CloseableIteration;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import mvm.rya.indexing.PrecompQueryIndexer;
import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet.AccValueFactory;

public class AccumuloPrecompQueryIndexer implements PrecompQueryIndexer {

    
    private Connector accCon;
    private String tableName;
    private Map<String, AccValueFactory> bindings;
  
    
    
    public AccumuloPrecompQueryIndexer(Connector accCon, String tableName) {
        this.accCon = accCon;
        this.tableName = tableName;
    }
    
    
    @Override
    public void storeBindingSet(BindingSet bs) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void storeBindingSets(Collection<BindingSet> bindingSets) throws IOException, IllegalArgumentException {
        // TODO Auto-generated method stub

    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> queryPrecompJoin(List<String> varOrder,
            String localityGroup, Map<String, AccValueFactory> bindings, Map<String, org.openrdf.model.Value> valMap, Collection<BindingSet> bsConstraints) 
                    throws QueryEvaluationException, TableNotFoundException {
        
        
        final int prefixLength = Integer.parseInt(varOrder.remove(varOrder.size()-1));
        final Iterator<Entry<Key,Value>> accIter;
        final HashMultimap<Range,BindingSet> map = HashMultimap.create();
        final List<BindingSet> extProdList = Lists.newArrayList();
        final Map<String, AccValueFactory> bindingMap = bindings;
        final List<String> order = varOrder;
        final BatchScanner bs = accCon.createBatchScanner(tableName, new Authorizations(), 10);
        final Set<Range> ranges = Sets.newHashSet();
        
        
        
        bs.fetchColumnFamily(new Text(localityGroup));
        
        //process bindingSet and constant constraints
        for (BindingSet bSet : bsConstraints) {
            StringBuffer rangePrefix = new StringBuffer();
            int i = 0;

            for (String b : order) {

                if (i >= prefixLength) {
                    break;
                }

                if (b.startsWith("-const-")) {
                    String val = bindings.get(b).create(valMap.get(b));
                    rangePrefix.append(val);
                    rangePrefix.append("\u0000");
                } else {

                    Binding v = bSet.getBinding(b);
                    if (v == null) {
                        throw new IllegalStateException("Binding set can't have null value!");
                    }
                    String val = bindings.get(b).create(bSet.getValue(b));
                    rangePrefix.append(val);
                    rangePrefix.append("\u0000");

                }

                i++;

            }
            if (rangePrefix.length() > 0) {
                String prefixWithOutNull = rangePrefix.deleteCharAt(rangePrefix.length() - 1).toString();
                String prefixWithNull = prefixWithOutNull + "\u0001";
                Range r = new Range(new Key(prefixWithOutNull), true, new Key(prefixWithNull), false);
                map.put(r, bSet);
                ranges.add(r);
            } else if (bSet.size() > 0) {
                extProdList.add(bSet);
            }
        }
        
        //constant constraints and no bindingSet constraints
        //add range of entire table if no constant constraints and
        //bsConstraints consists of single, empty set (occurs when AIS is
        //first node evaluated in query)
        if (ranges.isEmpty() && bsConstraints.size() > 0) {

            if (prefixLength > 0) {
                StringBuffer rangePrefix = new StringBuffer();

                int i = 0;
                for (String b : order) {
                    if (i >= prefixLength) {
                        break;
                    }
                    if (b.startsWith("-const-")) {
                        String val = bindings.get(b).create(valMap.get(b));
                        rangePrefix.append(val);
                        rangePrefix.append("\u0000");
                    } 
                    i++;
                }

                String prefixWithOutNull = rangePrefix.deleteCharAt(rangePrefix.length() - 1).toString();
                String prefixWithNull = prefixWithOutNull + "\u0001";
                Range r = new Range(new Key(prefixWithOutNull), true, new Key(prefixWithNull), false);
                ranges.add(r);

            } else { // no constant or bindingSet constraints
                ranges.add(new Range("", true, "~", false));
            }
        }
        
        if (ranges.size() == 0) {
            accIter = null;
        } else {
            bs.setRanges(ranges);
            accIter = bs.iterator();
        }

   
        return new CloseableIteration<BindingSet, QueryEvaluationException>() {

            @Override
            public void remove() throws QueryEvaluationException {
                throw new UnsupportedOperationException();
            }

            private Iterator<BindingSet> inputSet = null;
            private QueryBindingSet currentSolutionBs = null;
            private boolean hasNextCalled = false;
            private boolean isEmpty = false;
           

            
            @Override
            public BindingSet next() throws QueryEvaluationException {
                QueryBindingSet bs = new QueryBindingSet();

                if (hasNextCalled) {
                    hasNextCalled = false;
                    if (inputSet != null) {
                        bs.addAll(inputSet.next());
                    }
                    bs.addAll(currentSolutionBs);
                } else if (isEmpty) {
                    throw new NoSuchElementException();
                } else {
                    if (this.hasNext()) {
                        hasNextCalled = false;
                        if (inputSet != null) {
                            bs.addAll(inputSet.next());
                        }
                        bs.addAll(currentSolutionBs);
                    } else {
                        throw new NoSuchElementException();
                    }
                }

                return bs;
            }

            @Override
            public boolean hasNext() throws QueryEvaluationException {

                if(accIter == null ) {
                    isEmpty = true;
                    return false;
                }
                
                if (!hasNextCalled && !isEmpty) {
                    while (accIter.hasNext() || (inputSet != null && inputSet.hasNext())) {

                        if(inputSet != null && inputSet.hasNext()) {
                            hasNextCalled = true;
                            return true;
                        }
                        
                        
                        Key k = accIter.next().getKey();
                        final String[] s = k.getRow().toString().split("\u0000");
                       
                        StringBuilder rangePrefix = new StringBuilder();
                        // TODO Assuming that order specifies order of variables
                        // commmon to
                        // bindingSet passed in and variables in index table
                        // --size is equal to
                        
                        for (int i = 0; i < prefixLength; i++) {
                            rangePrefix.append(s[i]);
                            rangePrefix.append("\u0000");
                        }

                        // TODO I need to remember what the type was!
                        currentSolutionBs = new QueryBindingSet();
                        int i = 0;
                        for (String b : order) {
                            if (b.startsWith("-const")) {
                                i++;
                            } else {
                                final String v = s[i];
                                currentSolutionBs.addBinding(b, bindingMap.get(b).create(v));
                                i++;
                            }

                        }
                        //check to see if bindingSet constraints exist
                        if (map.size() > 0) {
                            String prefixWithOutNull = rangePrefix.deleteCharAt(rangePrefix.length() - 1).toString();
                            String prefixWithNull = prefixWithOutNull + "\u0001";
                            Range r = new Range(new Key(prefixWithOutNull), true, new Key(prefixWithNull), false);
                            inputSet = map.get(r).iterator();
                            if (!inputSet.hasNext()) {
                                continue;
                            } else {
                                hasNextCalled = true;
                                return true;
                            } // check to see if binding set constraints exist, but no common vars
                        } else if (extProdList.size() > 0) {
                            inputSet = extProdList.iterator();
                            hasNextCalled = true;
                            return true;
                        }else {  //no bindingsSet constraints--only constant constraints or none
                            hasNextCalled = true;
                            return true;
                        }
                    }

                    isEmpty = true;
                    return false;

                } else if (isEmpty) {
                    return false;
                } else {
                    return true;
                }

            }

            @Override
            public void close() throws QueryEvaluationException {
                bs.close();
            }

        };
    }

    
    
    @Override
    public void flush() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }
    
    
    
    
    

}
