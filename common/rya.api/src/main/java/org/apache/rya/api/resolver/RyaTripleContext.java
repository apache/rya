package org.apache.rya.api.resolver;

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



import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.query.strategy.TriplePatternStrategy;
import org.apache.rya.api.query.strategy.wholerow.HashedPoWholeRowTriplePatternStrategy;
import org.apache.rya.api.query.strategy.wholerow.HashedSpoWholeRowTriplePatternStrategy;
import org.apache.rya.api.query.strategy.wholerow.OspWholeRowTriplePatternStrategy;
import org.apache.rya.api.query.strategy.wholerow.PoWholeRowTriplePatternStrategy;
import org.apache.rya.api.query.strategy.wholerow.SpoWholeRowTriplePatternStrategy;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolver;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.api.resolver.triple.impl.WholeRowHashedTripleResolver;
import org.apache.rya.api.resolver.triple.impl.WholeRowTripleResolver;

/**
 * Date: 7/16/12
 * Time: 12:04 PM
 */
public class RyaTripleContext {

    public Log logger = LogFactory.getLog(RyaTripleContext.class);
    private TripleRowResolver tripleResolver;
    private final List<TriplePatternStrategy> triplePatternStrategyList = new ArrayList<TriplePatternStrategy>();

    public RyaTripleContext(final boolean addPrefixHash) {
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

    public synchronized static RyaTripleContext getInstance(final RdfCloudTripleStoreConfiguration conf) {
    	if (conf.isPrefixRowsWithHash()){
    		return RyaTripleContextHolder.HASHED_INSTANCE;
    	}
        return RyaTripleContextHolder.INSTANCE;
    }


     public Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serializeTriple(final RyaStatement statement) throws TripleRowResolverException {
        return getTripleResolver().serialize(statement);
    }

    public RyaStatement deserializeTriple(final RdfCloudTripleStoreConstants.TABLE_LAYOUT table_layout, final TripleRow tripleRow) throws TripleRowResolverException {
        return getTripleResolver().deserialize(table_layout, tripleRow);
    }

    protected void addDefaultTriplePatternStrategies(final boolean addPrefixHash) {
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
    public TriplePatternStrategy retrieveStrategy(final RyaURI subject, final RyaURI predicate, final RyaType object, final RyaURI context) {
        for (final TriplePatternStrategy strategy : triplePatternStrategyList) {
            if (strategy.handles(subject, predicate, object, context)) {
                return strategy;
            }
        }
        return null;
    }

    public TriplePatternStrategy retrieveStrategy(final RyaStatement stmt) {
        return retrieveStrategy(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext());
    }

    public TriplePatternStrategy retrieveStrategy(final TABLE_LAYOUT layout) {
        for(final TriplePatternStrategy strategy : triplePatternStrategyList) {
            if (strategy.getLayout().equals(layout)) {
                return strategy;
            }
        }
        return null;
    }

   public TripleRowResolver getTripleResolver() {
        return tripleResolver;
    }

    public void setTripleResolver(final TripleRowResolver tripleResolver) {
        this.tripleResolver = tripleResolver;
    }
}
