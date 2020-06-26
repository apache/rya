package org.apache.rya.api.utils;

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

import org.apache.rya.api.domain.RyaStatement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

/**
 * Class WildStatement
 *
 * Date: Feb 23, 2011
 * Time: 10:37:34 AM
 */
public class WildcardStatement implements Statement {

    private final Resource subject;
    private final IRI predicate;
    private final Value object;
    private final Resource context;

    /**
     * Creates a new Statement with the supplied subject, predicate and object.
     *
     * @param subject
     *        The statement's subject, may be <tt>null</tt>.
     * @param predicate
     *        The statement's predicate, may be <tt>null</tt>.
     * @param object
     *        The statement's object, may be <tt>null</tt>.
     */
    public WildcardStatement(Resource subject, IRI predicate, Value object) {
        this(subject, predicate, object, null);
    }

    /**
     * Creates a new Statement with the supplied subject, predicate and object for the specified
     * associated context.
     *
     * @param subject
     *        The statement's subject, may be <tt>null</tt>.
     * @param predicate
     *        The statement's predicate, may be <tt>null</tt>.
     * @param object
     *        The statement's object, may be <tt>null</tt>.
     * @param context
     *        The statement's context, <tt>null</tt> to indicate no context is associated.
     */
    public WildcardStatement(Resource subject, IRI predicate, Value object, Resource context) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
        this.context = context;
    }

    public WildcardStatement(RyaStatement statement) {
        this(statement.getSubject(), statement.getPredicate(), statement.getObject(), statement.getContext());
    }

    @Override
    public int hashCode() {
        int result = subject != null ? subject.hashCode() : 0;
        result = 31 * result + (predicate != null ? predicate.hashCode() : 0);
        result = 31 * result + (object != null ? object.hashCode() : 0);
        result = 31 * result + (context != null ? context.hashCode() : 0);
        //result = 31 * result + (contexts.length > 0 ? Arrays.hashCode(contexts) : 0);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(256);

        sb.append("(");
        sb.append(getSubject());
        sb.append(", ");
        sb.append(getPredicate());
        sb.append(", ");
        sb.append(getObject());
        sb.append(")");
        sb.append(" [").append(getContext()).append("]");

        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other instanceof WildcardStatement) {
            WildcardStatement otherSt = (WildcardStatement) other;
            return this.hashCode() == otherSt.hashCode();
        } else {
            return false;
        }
    }

    // Implements Statement.getSubject()
    public Resource getSubject() {
        return subject;
    }

    // Implements Statement.getPredicate()
    public IRI getPredicate() {
        return predicate;
    }

    // Implements Statement.getObject()
    public Value getObject() {
        return object;
    }

    @Override
    public Resource getContext() {
        return context;
    }

}
