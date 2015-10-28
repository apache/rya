package mvm.rya.api.resolver;

/*
 * #%L
 * mvm.rya.rya.api
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.query.strategy.TriplePatternStrategy;
import mvm.rya.api.query.strategy.wholerow.HashedPoWholeRowTriplePatternStrategy;
import mvm.rya.api.query.strategy.wholerow.HashedSpoWholeRowTriplePatternStrategy;
import mvm.rya.api.query.strategy.wholerow.OspWholeRowTriplePatternStrategy;
import mvm.rya.api.query.strategy.wholerow.PoWholeRowTriplePatternStrategy;
import mvm.rya.api.query.strategy.wholerow.SpoWholeRowTriplePatternStrategy;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolver;
import mvm.rya.api.resolver.triple.TripleRowResolverException;
import mvm.rya.api.resolver.triple.impl.WholeRowHashedTripleResolver;
import mvm.rya.api.resolver.triple.impl.WholeRowTripleResolver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Date: 7/16/12
 * Time: 12:04 PM
 */
public class RyaTripleContext {

    public Log logger = LogFactory.getLog(RyaTripleContext.class);
    private TripleRowResolver tripleResolver;
    private List<TriplePatternStrategy> triplePatternStrategyList = new ArrayList<TriplePatternStrategy>();

    private RyaTripleContext(boolean addPrefixHash) {
        addDefaultTriplePatternStrategies(addPrefixHash);
        if (addPrefixHash){
        	tripleResolver = new WholeRowHashedTripleResolver();
        }
        else {
        	tripleResolver = new WholeRowTripleResolver();
        }
    }


    private static class RyaTripleContextHolder {
    	// TODO want to be able to support more variability in configuration here
        public static final RyaTripleContext INSTANCE = new RyaTripleContext(false);
        public static final RyaTripleContext HASHED_INSTANCE = new RyaTripleContext(true);
    }

    public synchronized static RyaTripleContext getInstance(RdfCloudTripleStoreConfiguration conf) {
    	if (conf.isPrefixRowsWithHash()){
    		return RyaTripleContextHolder.HASHED_INSTANCE;
    	}
        return RyaTripleContextHolder.INSTANCE;
    }
    

     public Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serializeTriple(RyaStatement statement) throws TripleRowResolverException {
        return getTripleResolver().serialize(statement);
    }

    public RyaStatement deserializeTriple(RdfCloudTripleStoreConstants.TABLE_LAYOUT table_layout, TripleRow tripleRow) throws TripleRowResolverException {
        return getTripleResolver().deserialize(table_layout, tripleRow);
    }

    protected void addDefaultTriplePatternStrategies(boolean addPrefixHash) {
    	if (addPrefixHash){
            triplePatternStrategyList.add(new HashedSpoWholeRowTriplePatternStrategy());
            triplePatternStrategyList.add(new HashedPoWholeRowTriplePatternStrategy());
    	}
    	else {
            triplePatternStrategyList.add(new SpoWholeRowTriplePatternStrategy());
            triplePatternStrategyList.add(new PoWholeRowTriplePatternStrategy());
    	}
        triplePatternStrategyList.add(new OspWholeRowTriplePatternStrategy());
    }

    //retrieve triple pattern strategy
    public TriplePatternStrategy retrieveStrategy(RyaURI subject, RyaURI predicate, RyaType object, RyaURI context) {
        for (TriplePatternStrategy strategy : triplePatternStrategyList) {
            if (strategy.handles(subject, predicate, object, context))
                return strategy;
        }
        return null;
    }

    public TriplePatternStrategy retrieveStrategy(RyaStatement stmt) {
        return retrieveStrategy(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext());
    }

    public TripleRowResolver getTripleResolver() {
        return tripleResolver;
    }

    public void setTripleResolver(TripleRowResolver tripleResolver) {
        this.tripleResolver = tripleResolver;
    }
}
