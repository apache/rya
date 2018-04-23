package org.apache.rya.indexing.statement.metadata.matching;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.external.matching.ExternalSetProvider;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * This class extracts all valid {@Link StatementMetadataNode}s from the provided {@link QuerySegment}s.
 *
 */
public class StatementMetadataExternalSetProvider implements ExternalSetProvider<StatementMetadataNode<?>> {

    private List<RyaIRI> expectedURI = Arrays.asList(RdfToRyaConversions.convertIRI(OWLReify.SOURCE),
            RdfToRyaConversions.convertIRI(OWLReify.PROPERTY), RdfToRyaConversions.convertIRI(OWLReify.TARGET),
            RdfToRyaConversions.convertIRI(RDF.TYPE));
    private Multimap<Var, StatementPattern> reifiedQueries;
    private Set<RyaIRI> metadataProperties;
    private RdfCloudTripleStoreConfiguration conf;

    public StatementMetadataExternalSetProvider(RdfCloudTripleStoreConfiguration conf) {
        this.metadataProperties = conf.getStatementMetadataProperties();
        this.conf = conf;
    }

    /**
     * This method extracts all {@link StatementMetadataNode}s from the provided {@link QuerySegment}.
     * It looks through the provided QuerySegment for all combinations of {@link StatementPattern}s that 
     * represent a reified query and combines those into a StatementPatternNode.  A StatementPattern cannot
     * be used in more than one reified query and StatementPatternNode.
     */
    @Override
    public List<StatementMetadataNode<?>> getExternalSets(QuerySegment<StatementMetadataNode<?>> segment) {

        reifiedQueries = HashMultimap.create();

        List<StatementMetadataNode<?>> metadataList = new ArrayList<>();
        for (QueryModelNode node : segment.getUnOrderedNodes()) {
            if (node instanceof StatementPattern) {
                StatementPattern sp = (StatementPattern) node;
                reifiedQueries.put(sp.getSubjectVar(), sp);
            }
        }

        for (Var var : reifiedQueries.keySet()) {
            Collection<StatementPattern> patterns = removeInvalidProperties(reifiedQueries.get(var));
            if (StatementMetadataNode.verifyHasCorrectTypePattern(patterns)) {
                metadataList.add(new StatementMetadataNode<>(patterns, conf));
            }
        }

        return metadataList;
    }

    @Override
    public Iterator<List<StatementMetadataNode<?>>> getExternalSetCombos(
            QuerySegment<StatementMetadataNode<?>> segment) {
        Set<List<StatementMetadataNode<?>>> combos = new HashSet<>();
        combos.add(getExternalSets(segment));
        return combos.iterator();
    }

    private Set<StatementPattern> removeInvalidProperties(Collection<StatementPattern> patterns) {

        Set<StatementPattern> finalPatterns = new HashSet<>();
        
        for (StatementPattern pattern : patterns) {
            Var var = pattern.getPredicateVar();
            if (var.getValue() != null && var.getValue() instanceof IRI) {
                RyaIRI uri = RdfToRyaConversions.convertIRI((IRI) var.getValue());
                if(expectedURI.contains(uri) || metadataProperties.contains(uri)) {
                    finalPatterns.add(pattern);
                }
            }
        }
        return finalPatterns;
    }

}
