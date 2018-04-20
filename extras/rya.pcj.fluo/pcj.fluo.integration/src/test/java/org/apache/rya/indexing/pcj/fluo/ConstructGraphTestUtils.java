package org.apache.rya.indexing.pcj.fluo;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.eclipse.rdf4j.model.Statement;
import org.junit.Assert;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class ConstructGraphTestUtils {

    public static void ryaStatementSetsEqualIgnoresTimestamp(Set<RyaStatement> statements1, Set<RyaStatement> statements2) {
        Assert.assertEquals(new VisibilityStatementSet(statements1), new VisibilityStatementSet(statements2));
    }

    public static void subGraphsEqualIgnoresTimestamp(Set<RyaSubGraph> subgraph1, Set<RyaSubGraph> subgraph2) {
        Set<VisibilityStatementSet> set1 = new HashSet<>();
        Set<VisibilityStatementSet> set2 = new HashSet<>();
        subgraph1.forEach(x->set1.add(new VisibilityStatementSet(x.getStatements())));
        subgraph2.forEach(x->set2.add(new VisibilityStatementSet(x.getStatements())));
        Assert.assertEquals(set1, set2);
    }
    
    public static void subGraphsEqualIgnoresBlankNode(Set<RyaSubGraph> subgraph1, Set<RyaSubGraph> subgraph2) {
        Map<Integer, RyaSubGraph> subGraphMap = new HashMap<>();
        for(RyaSubGraph subgraph: subgraph1) {
            int key = getKey(subgraph);
            subGraphMap.put(key, subgraph);
        }
        
        for(RyaSubGraph subgraph: subgraph2) {
            int key = getKey(subgraph);
            RyaSubGraph sub = subGraphMap.get(key);
            Preconditions.checkNotNull(sub);
            Set<RyaStatement> statements = sub.getStatements();
            ryaStatementsEqualIgnoresBlankNode(subgraph.getStatements(), statements);
        }
    }
    
    private static int getKey(RyaSubGraph subgraph) {
        int key = 0;
        for(RyaStatement statement: subgraph.getStatements()) {
            key += statement.getObject().hashCode();
        }
        return key;
    }
    
    public static void ryaStatementsEqualIgnoresBlankNode(Set<RyaStatement> statements1, Set<RyaStatement> statements2) {
        Map<String, RyaIRI> bNodeMap = new HashMap<>();
        statements1.forEach(x-> bNodeMap.put(x.getPredicate().getData(), x.getSubject()));
        statements2.forEach(x -> x.setSubject(bNodeMap.get(x.getPredicate().getData())));
        ryaStatementSetsEqualIgnoresTimestamp(statements1, statements2);
    }
    
    
    /**
     *  Class used for comparing Sets of RyaStatements while ignoring timestamps.
     *  It is assumed that all RyaStatements in the Set used to construct this class
     *  have the same visibility.
     */
    public static class VisibilityStatementSet {
        
        private Set<Statement> statements;
        private String visibility;
        
        public VisibilityStatementSet(Set<RyaStatement> statements) {
            this.statements = new HashSet<>();
            statements.forEach(x -> {
                this.statements.add(RyaToRdfConversions.convertStatement(x));
                if (visibility == null) {
                    if (x.getColumnVisibility() != null) {
                        visibility = new String(x.getColumnVisibility());
                    } else {
                        this.visibility = "";
                    }
                }
            });
        }
        
        public VisibilityStatementSet(RyaSubGraph subgraph) {
            this(subgraph.getStatements());
        }
        
        @Override
        public boolean equals(Object o) {
            if(this == o) {
                return true;
            }
            
            if(o instanceof VisibilityStatementSet) {
                VisibilityStatementSet that = (VisibilityStatementSet) o;
                return Objects.equal(this.visibility, that.visibility) && Objects.equal(this.statements, that.statements);
            }
            
            return false;
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(visibility, statements);
        }
        
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            return builder.append("Visiblity Statement Set \n").append("   Statements: " + statements + "\n")
                    .append("   Visibilities: " + visibility + " \n").toString();
        }
        
    }
    
}
