package org.apache.rya.indexing.statement.metadata;
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.statement.metadata.matching.OWLReify;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class StatementMetadataTestUtils {

    private static final List<RyaURI> uriList = Arrays.asList(new RyaURI(RDF.TYPE.toString()),
            new RyaURI(OWLReify.SOURCE.toString()), new RyaURI(OWLReify.PROPERTY.toString()),
            new RyaURI(OWLReify.TARGET.toString()));

    public static Set<QueryModelNode> getMetadataNodes(TupleExpr query) {
        MetadataNodeCollector collector = new MetadataNodeCollector();
        query.visit(collector);
        return collector.getNodes();
    }

    public static class MetadataNodeCollector extends AbstractQueryModelVisitor<RuntimeException> {

        Set<QueryModelNode> qNodes = new HashSet<>();

        @Override
        public void meetNode(final QueryModelNode node) {
            if (node instanceof ExternalSet) {
                qNodes.add(node);
            }
            super.meetNode(node);
        }

        public Set<QueryModelNode> getNodes() {
            return qNodes;
        }
    }

    public static Set<StatementPattern> getMetadataStatementPatterns(TupleExpr te, Set<RyaURI> properties) {
        MetadataStatementPatternCollector collector = new MetadataStatementPatternCollector(properties);
        te.visit(collector);
        return collector.getNodes();

    }

    public static class MetadataStatementPatternCollector extends AbstractQueryModelVisitor<RuntimeException> {

        private Set<StatementPattern> nodes;
        private Set<RyaURI> properties;

        public MetadataStatementPatternCollector(Set<RyaURI> properties) {
            this.properties = properties;
            nodes = new HashSet<>();
        }

        @Override
        public void meet(StatementPattern node) {
            Var predicate = node.getPredicateVar();
            Value val = predicate.getValue();
            if (val != null && val instanceof IRI) {
                RyaURI ryaVal = new RyaURI(val.stringValue());
                if (uriList.contains(ryaVal) || properties.contains(ryaVal)) {
                    nodes.add(node);
                }
            }
            super.meet(node);
        }

        public Set<StatementPattern> getNodes() {
            return nodes;
        }
    }

}
