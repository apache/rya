package org.apache.rya.api.domain;
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
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Objects;

/**
 * This class packages together a collection of {@link RyaStatement}s to form a subgraph
 */
public class RyaSubGraph {

    private String id;
    private Set<RyaStatement> statements;
    
    /**
     * Creates empty subgraph with given id
     * @param id - id of the created subgraph
     */
    public RyaSubGraph(String id) {
        this.id = id;
        this.statements = new HashSet<>();
    }
    
    /**
     * Creates sugraph with specified id and statements
     * @param id - id of the created subgraph
     * @param statements - statements that make up subgraph
     */
    public RyaSubGraph(String id, Set<RyaStatement> statements) {
        this.id = id;
        this.statements = statements;
    }

    /**
     * @return id of this subgraph
     */
    public String getId() {
        return id;
    }
    
    /**
     * @return RyaStatements representing this subgraph
     */
    public Set<RyaStatement> getStatements() {
        return statements;
    }
    
    /**
     * Sets id of subgraph
     * @param id - id of subgraph
     */
    public void setId(String id) {
        this.id = id;
    }
    
    /**
     * Sets subgraph statements to specified statements
     * @param statements - statements that will be set to subgraph statements
     */
    public void setStatements(Set<RyaStatement> statements) {
        this.statements = statements;
    }
    

    /**
     * Adds statement to this subgraph
     * @param statement - RyaStatement to be added to subgraph
     */
    public void addStatement(RyaStatement statement){
        statements.add(statement);
    }
    
    @Override
    public boolean equals(Object other) {
        
        if(this == other) {
            return true;
        }
        
        if(other instanceof RyaSubGraph) {
            RyaSubGraph bundle = (RyaSubGraph) other;
            return Objects.equal(this.id, ((RyaSubGraph) other).id) && Objects.equal(this.statements,bundle.statements);
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(this.id, this.statements);
    }
    
    
    @Override
    public String toString() {
        return new StringBuilder().append("Rya Subgraph {\n").append("   Rya Subgraph ID: " + id + "\n")
                .append("   Rya Statements: " + statements + "\n").toString();
    }
    
}