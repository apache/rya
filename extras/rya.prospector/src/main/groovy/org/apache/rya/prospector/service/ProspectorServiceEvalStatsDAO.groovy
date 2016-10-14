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

package org.apache.rya.prospector.service

import org.apache.rya.api.RdfCloudTripleStoreConfiguration
import org.apache.rya.api.persist.RdfEvalStatsDAO
import org.apache.rya.prospector.domain.TripleValueType
import org.apache.rya.prospector.utils.ProspectorConstants
import org.apache.hadoop.conf.Configuration
import org.openrdf.model.Resource
import org.openrdf.model.Value

import org.apache.rya.api.persist.RdfEvalStatsDAO.CARDINALITY_OF

/**
 * An ${@link org.apache.rya.api.persist.RdfEvalStatsDAO} that uses the Prospector Service underneath return counts.
 */
class ProspectorServiceEvalStatsDAO implements RdfEvalStatsDAO<RdfCloudTripleStoreConfiguration> {

    def ProspectorService prospectorService

    ProspectorServiceEvalStatsDAO() {
    }

    ProspectorServiceEvalStatsDAO(ProspectorService prospectorService, RdfCloudTripleStoreConfiguration conf) {
        this.prospectorService = prospectorService
    }

    public ProspectorServiceEvalStatsDAO(def connector, RdfCloudTripleStoreConfiguration conf) {
        this.prospectorService = new ProspectorService(connector, getProspectTableName(conf))
    }

    @Override
    void init() {
        assert prospectorService != null
    }

    @Override
    boolean isInitialized() {
        return prospectorService != null
    }

    @Override
    void destroy() {

    }

	@Override
    public double getCardinality(RdfCloudTripleStoreConfiguration conf, CARDINALITY_OF card, List<Value> val) {

        assert conf != null && card != null && val != null
        String triplePart = null;
        switch (card) {
            case (CARDINALITY_OF.SUBJECT):
                triplePart = TripleValueType.subject
                break;
            case (CARDINALITY_OF.PREDICATE):
                triplePart = TripleValueType.predicate
                break;
            case (CARDINALITY_OF.OBJECT):
                triplePart = TripleValueType.object
                break;
            case (CARDINALITY_OF.SUBJECTPREDICATE):
                triplePart = TripleValueType.subjectpredicate
                break;
             case (CARDINALITY_OF.SUBJECTOBJECT):
                triplePart = TripleValueType.subjectobject
                break;
             case (CARDINALITY_OF.PREDICATEOBJECT):
                triplePart = TripleValueType.predicateobject
                break;
       }

        String[] auths = conf.getAuths()
		List<String> indexedValues = new ArrayList<String>();
		Iterator<Value> valueIt = val.iterator();
		while (valueIt.hasNext()){
			indexedValues.add(valueIt.next().stringValue());
		}

        def indexEntries = prospectorService.query(null, ProspectorConstants.COUNT, triplePart, indexedValues, null /** what is the datatype here? */,
                auths)

        return indexEntries.size() > 0 ? indexEntries.head().count : -1
    }

	@Override
	double getCardinality(RdfCloudTripleStoreConfiguration conf, CARDINALITY_OF card, List<Value> val, Resource context) {
		return getCardinality(conf, card, val) //TODO: Not sure about the context yet
	}

    @Override
    public void setConf(RdfCloudTripleStoreConfiguration conf) {

    }

    @Override
    RdfCloudTripleStoreConfiguration getConf() {
        return null
    }

    public static String getProspectTableName(RdfCloudTripleStoreConfiguration conf) {
        return conf.getTablePrefix() + "prospects";
    }
}
