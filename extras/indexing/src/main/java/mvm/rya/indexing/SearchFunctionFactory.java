package mvm.rya.indexing;

import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import com.google.common.collect.Maps;

public abstract class SearchFunctionFactory {
    
    private static final Logger logger = Logger.getLogger(SearchFunctionFactory.class);

    private final Map<URI, SearchFunction> SEARCH_FUNCTION_MAP = Maps.newHashMap();


    /**
     * Get a {@link GeoSearchFunction} for a give URI.
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

   
}
  