package org.apache.rya.indexing;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.QueryEvaluationException;

public abstract class SearchFunctionFactory {
    
    private static final Logger logger = Logger.getLogger(SearchFunctionFactory.class);

    private final Map<IRI, SearchFunction> SEARCH_FUNCTION_MAP = Maps.newHashMap();


    /**
     * Get a {@link GeoSearchFunction} for a give URI.
     * 
     * @param searchFunction
     * @return
     */
    public SearchFunction getSearchFunction(final IRI searchFunction) {

        SearchFunction geoFunc = null;

        try {
            geoFunc = getSearchFunctionInternal(searchFunction);
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        }

        return geoFunc;
    }

    private SearchFunction getSearchFunctionInternal(final IRI searchFunction) throws QueryEvaluationException {
        SearchFunction sf = SEARCH_FUNCTION_MAP.get(searchFunction);

        if (sf != null) {
            return sf;
        } else {
            throw new QueryEvaluationException("Unknown Search Function: " + searchFunction.stringValue());
        }
            
       
    }

   
}
  
