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
package org.apache.rya.prospector.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.utils.ConnectorFactory;
import org.apache.rya.api.RdfTripleStoreConfiguration;
import org.apache.rya.api.persist.RdfDAOException;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.prospector.domain.IndexEntry;
import org.apache.rya.prospector.domain.TripleValueType;
import org.apache.rya.prospector.utils.ProspectorConstants;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

import static java.util.Objects.requireNonNull;

/**
 * An ${@link org.apache.rya.api.persist.RdfEvalStatsDAO} that uses the Prospector Service underneath return counts.
 */
public class ProspectorServiceEvalStatsDAO implements RdfEvalStatsDAO<RdfTripleStoreConfiguration> {

    private ProspectorService prospectorService;

    public ProspectorServiceEvalStatsDAO() {
    }

    public ProspectorServiceEvalStatsDAO(ProspectorService prospectorService, RdfTripleStoreConfiguration conf) {
        this.prospectorService = prospectorService;
    }

    public ProspectorServiceEvalStatsDAO(Connector connector, RdfTripleStoreConfiguration conf) throws AccumuloException, AccumuloSecurityException {
        this.prospectorService = new ProspectorService(connector, getProspectTableName(conf));
    }

    /**
     * Creates an instance of {@link ProspectorServiceEvalStatsDAO} that is connected to the Accumulo and Rya instance
     * that is specified within the provided {@link Configuration}.
     *
     * @param config - Configures which instance of Accumulo Rya this DAO will be backed by. (not null)
     * @return A new instance of {@link ProspectorServiceEvalStatsDAO}.
     * @throws AccumuloException The connector couldn't be created because of an Accumulo problem.
     * @throws AccumuloSecurityException The connector couldn't be created because of an Accumulo security violation.
     */
    public static ProspectorServiceEvalStatsDAO make(Configuration config) throws AccumuloException, AccumuloSecurityException {
        requireNonNull(config);
        AccumuloRdfConfiguration accConfig = new AccumuloRdfConfiguration(config);
        return new ProspectorServiceEvalStatsDAO(ConnectorFactory.connect(accConfig), accConfig);
    }


    @Override
    public void init() {
        assert prospectorService != null;
    }

    @Override
    public boolean isInitialized() {
        return prospectorService != null;
    }

    @Override
    public void destroy() {
    }

    @Override
    public double getCardinality(RdfTripleStoreConfiguration conf, CARDINALITY_OF card, List<Value> val) throws RdfDAOException {
        assert conf != null && card != null && val != null;

        String triplePart = null;
        switch (card) {
            case SUBJECT:
                triplePart = TripleValueType.SUBJECT.getIndexType();
                break;
            case PREDICATE:
                triplePart = TripleValueType.PREDICATE.getIndexType();
                break;
            case OBJECT:
                triplePart = TripleValueType.OBJECT.getIndexType();
                break;
            case SUBJECTPREDICATE:
                triplePart = TripleValueType.SUBJECT_PREDICATE.getIndexType();
                break;
            case SUBJECTOBJECT:
                triplePart = TripleValueType.SUBJECT_OBJECT.getIndexType();
                break;
            case PREDICATEOBJECT:
                triplePart = TripleValueType.PREDICATE_OBJECT.getIndexType();
                break;
        }

        final String[] auths = conf.getAuths();
        final List<String> indexedValues = new ArrayList<>();
        final Iterator<Value> valueIt = val.iterator();
        while (valueIt.hasNext()){
            indexedValues.add(valueIt.next().stringValue());
        }

        double cardinality = -1;
        try {
            final List<IndexEntry> entries = prospectorService.query(null, ProspectorConstants.COUNT, triplePart, indexedValues, null, auths);
            if(!entries.isEmpty()) {
                cardinality = entries.iterator().next().getCount();
            }
        } catch (final TableNotFoundException e) {
            throw new RdfDAOException(e);
        }
        return cardinality;
    }

    @Override
    public double getCardinality(RdfTripleStoreConfiguration conf, CARDINALITY_OF card, List<Value> val, Resource context) {
        return getCardinality(conf, card, val); //TODO: Not sure about the context yet
    }

    @Override
    public void setConf(RdfTripleStoreConfiguration conf) {
    }

    @Override
    public RdfTripleStoreConfiguration getConf() {
        return null;
    }

    public static String getProspectTableName(RdfTripleStoreConfiguration conf) {
        return conf.getTablePrefix() + "prospects";
    }

    /**
     * This method exists so that the Rya Web project may autowrire itself together
     * using the Spring framework.
     *
     * @param prospectorService - The {@link ProspectorService} that will be used by this DAO.
     */
    public void setProspectorService(ProspectorService prospectorService) {
        this.prospectorService = prospectorService;
    }
}