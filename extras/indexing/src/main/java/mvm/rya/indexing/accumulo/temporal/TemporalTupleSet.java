package mvm.rya.indexing.accumulo.temporal;

import info.aduna.iteration.CloseableIteration;

import java.util.Map;
import java.util.Set;

import mvm.rya.indexing.IndexingExpr;
import mvm.rya.indexing.IteratorFactory;
import mvm.rya.indexing.SearchFunction;
import mvm.rya.indexing.SearchFunctionFactory;
import mvm.rya.indexing.StatementContraints;
import mvm.rya.indexing.TemporalIndexer;
import mvm.rya.indexing.TemporalInstant;
import mvm.rya.indexing.TemporalInterval;
import mvm.rya.indexing.accumulo.geo.GeoTupleSet;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryModelVisitor;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

//Indexing Node for temporal expressions to be inserted into execution plan 
//to delegate temporal portion of query to temporal index
public class TemporalTupleSet extends ExternalTupleSet {

    private Configuration conf;
    private TemporalIndexer temporalIndexer;
    private IndexingExpr filterInfo;
    

    public TemporalTupleSet(IndexingExpr filterInfo, TemporalIndexer temporalIndexer) {
        this.filterInfo = filterInfo;
        this.temporalIndexer = temporalIndexer;
        this.conf = temporalIndexer.getConf();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getBindingNames() {
        return filterInfo.getBindingNames();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note that we need a deep copy for everything that (during optimizations)
     * can be altered via {@link #visitChildren(QueryModelVisitor)}
     */
    public TemporalTupleSet clone() {
        return new TemporalTupleSet(filterInfo, temporalIndexer);
    }

    @Override
    public double cardinality() {
        return 0.0; // No idea how the estimate cardinality here.
    }
    
    
    @Override
    public String getSignature() {
        
        return "(TemporalTuple Projection) " + "variables: " + Joiner.on(", ").join(this.getBindingNames()).replaceAll("\\s+", " ");
    }
    
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof TemporalTupleSet)) {
            return false;
        }
        TemporalTupleSet arg = (TemporalTupleSet) other;
        return this.filterInfo.equals(arg.filterInfo);
    }
    
    
    @Override
    public int hashCode() {
        int result = 17;
        result = 31*result + filterInfo.hashCode();
        
        return result;
    }
    

    /**
     * Returns an iterator over the result set associated with contained IndexingExpr.
     * <p>
     * Should be thread-safe (concurrent invocation {@link OfflineIterable} this
     * method can be expected with some query evaluators.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings)
            throws QueryEvaluationException {
        
      
        URI funcURI = filterInfo.getFunction();
        SearchFunction searchFunction = (new TemporalSearchFunctionFactory(conf)).getSearchFunction(funcURI);

        if(filterInfo.getArguments().length > 1) {
            throw new IllegalArgumentException("Index functions do not support more than two arguments.");
        }
        
        String queryText = filterInfo.getArguments()[0].stringValue();
        
        return IteratorFactory.getIterator(filterInfo.getSpConstraint(), bindings, queryText, searchFunction);
    }

    
    //returns appropriate search function for a given URI
    //search functions used by TemporalIndexer to query Temporal Index
    private class TemporalSearchFunctionFactory  {

        private final Map<URI, SearchFunction> SEARCH_FUNCTION_MAP = Maps.newHashMap();
        Configuration conf;

        public TemporalSearchFunctionFactory(Configuration conf) {
            this.conf = conf;
        }


        /**
         * Get a {@link TemporalSearchFunction} for a give URI.
         * 
         * @param searchFunction
         * @return
         */
        public SearchFunction getSearchFunction(final URI searchFunction) {

            SearchFunction geoFunc = null;

            try {
                geoFunc = getSearchFunctionInternal(searchFunction);
            } catch (QueryEvaluationException e) {
                e.printStackTrace();
            }

            return geoFunc;
        }

        private SearchFunction getSearchFunctionInternal(final URI searchFunction) throws QueryEvaluationException {
            SearchFunction sf = SEARCH_FUNCTION_MAP.get(searchFunction);

            if (sf != null) {
                return sf;
            } else {
                throw new QueryEvaluationException("Unknown Search Function: " + searchFunction.stringValue());
            }
                
           
        }

       
        
        private final SearchFunction TEMPORAL_InstantAfterInstant = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(String searchTerms,
                    StatementContraints contraints) throws QueryEvaluationException {
                TemporalInstant queryInstant = new TemporalInstantRfc3339(DateTime.parse(searchTerms));
                return temporalIndexer.queryInstantAfterInstant(queryInstant, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantAfterInstant";
            };
        };
        private final SearchFunction TEMPORAL_InstantBeforeInstant = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(String searchTerms,
                    StatementContraints contraints) throws QueryEvaluationException {
                TemporalInstant queryInstant = new TemporalInstantRfc3339(DateTime.parse(searchTerms));
                return temporalIndexer.queryInstantBeforeInstant(queryInstant, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantBeforeInstant";
            };
        };

        private final SearchFunction TEMPORAL_InstantEqualsInstant = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(String searchTerms,
                    StatementContraints contraints) throws QueryEvaluationException {
                TemporalInstant queryInstant = new TemporalInstantRfc3339(DateTime.parse(searchTerms));
                return temporalIndexer.queryInstantEqualsInstant(queryInstant, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantEqualsInstant";
            };
        };

        private final SearchFunction TEMPORAL_InstantAfterInterval = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(String searchTerms,
                    StatementContraints contraints) throws QueryEvaluationException {
                TemporalInterval queryInterval = TemporalInstantRfc3339.parseInterval(searchTerms);
                return temporalIndexer.queryInstantAfterInterval(queryInterval, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantAfterInterval";
            };
        };

        private final SearchFunction TEMPORAL_InstantBeforeInterval = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(String searchTerms,
                    StatementContraints contraints) throws QueryEvaluationException {
                TemporalInterval queryInterval = TemporalInstantRfc3339.parseInterval(searchTerms);
                return temporalIndexer.queryInstantBeforeInterval(queryInterval, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantBeforeInterval";
            };
        };

        private final SearchFunction TEMPORAL_InstantInsideInterval = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(String searchTerms,
                    StatementContraints contraints) throws QueryEvaluationException {
                TemporalInterval queryInterval = TemporalInstantRfc3339.parseInterval(searchTerms);
                return temporalIndexer.queryInstantInsideInterval(queryInterval, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantInsideInterval";
            };
        };

        private final SearchFunction TEMPORAL_InstantHasBeginningInterval = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(String searchTerms,
                    StatementContraints contraints) throws QueryEvaluationException {
                TemporalInterval queryInterval = TemporalInstantRfc3339.parseInterval(searchTerms);
                return temporalIndexer.queryInstantHasBeginningInterval(queryInterval, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantHasBeginningInterval";
            };
        };

        private final SearchFunction TEMPORAL_InstantHasEndInterval = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(String searchTerms,
                    StatementContraints contraints) throws QueryEvaluationException {
                TemporalInterval queryInterval = TemporalInstantRfc3339.parseInterval(searchTerms);
                return temporalIndexer.queryInstantHasEndInterval(queryInterval, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantHasEndInterval";
            };
        };

        {
            
            String TEMPORAL_NS = "tag:rya-rdf.org,2015:temporal#";         

            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"after"), TEMPORAL_InstantAfterInstant);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"before"), TEMPORAL_InstantBeforeInstant);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"equals"), TEMPORAL_InstantEqualsInstant);

            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"beforeInterval"), TEMPORAL_InstantBeforeInterval);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"afterInterval"), TEMPORAL_InstantAfterInterval);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"insideInterval"), TEMPORAL_InstantInsideInterval);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"hasBeginningInterval"),
                    TEMPORAL_InstantHasBeginningInterval);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"hasEndInterval"), TEMPORAL_InstantHasEndInterval);

        }
        
        
        

    }

}
