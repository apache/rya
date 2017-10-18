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
package org.apache.rya.api.model;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An abstract class used to decorate a {@link Statement}.
 */
@DefaultAnnotation(NonNull.class)
public abstract class StatementDecorator implements Statement {
    private static final long serialVersionUID = 1L;

    private final Statement statement;

    /**
     * Constructs an instance of {@link StatementDecorator}.
     *
     * @param statement - The {@link Statement} that will be decorated. (not null)
     */
    public StatementDecorator(final Statement statement) {
        this.statement = requireNonNull(statement);
    }

    /**
     * @return The decorated {@link Statement}.
     */
    public Statement getStatement() {
        return statement;
    }

    @Override
    public Resource getSubject() {
        return statement.getSubject();
    }

    @Override
    public IRI getPredicate() {
        return statement.getPredicate();
    }

    @Override
    public Value getObject() {
        return statement.getObject();
    }

    @Override
    public Resource getContext() {
        return statement.getContext();
    }

    @Override
    public boolean equals(final Object o) {
        if(o instanceof StatementDecorator) {
            final StatementDecorator other = (StatementDecorator) o;
            return Objects.equals(statement, other.statement);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return statement.hashCode();
    }
}