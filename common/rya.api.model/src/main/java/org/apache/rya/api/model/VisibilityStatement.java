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

import org.openrdf.model.Statement;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Decorates a {@link Statement} with a visibility expression.
 */
@DefaultAnnotation(NonNull.class)
public class VisibilityStatement extends StatementDecorator {
    private static final long serialVersionUID = 1L;

    private String visibility;

    /**
     * Constructs an instance of {@link VisibilityStatement} with an empty visibility expression.
     *
     * @param statement - The statement that will be decorated. (not null)
     */
    public VisibilityStatement(final Statement statement) {
        this(statement, "");
    }

    /**
     * Constructs an instance of {@link VisibilityStatement}.
     *
     * @param statement - The statement that will be decorated. (not null)
     * @param visibility - The visibility expression associated with the statement. (not null)
     */
    public VisibilityStatement(final Statement statement, final String visibility) {
        super(statement);
        this.visibility = requireNonNull(visibility);
    }

    /**
     * @param visibility - The visibility expression. (not null)
     */
    public void setVisibility(final String visibility) {
        this.visibility = requireNonNull(visibility);
    }

    /**
     * @return The visibility expression.
     */
    public String getVisibility() {
        return visibility;
    }

    @Override
    public boolean equals(final Object o) {
        if(o instanceof VisibilityStatement) {
            final VisibilityStatement other = (VisibilityStatement) o;
            return Objects.equals(visibility, other.visibility) &&
                    Objects.equals(super.getStatement(), other.getStatement());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(visibility, super.getStatement());
    }
}