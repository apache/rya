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



import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

/**
 * Class NullableStatementImpl
 * Date: Feb 23, 2011
 * Time: 10:37:34 AM
 */
public class NullableStatementImpl implements Statement {

    private Resource subject;
    private URI predicate;
    private Value object;
    private Resource[] contexts;

    public NullableStatementImpl(Resource subject, URI predicate, Value object, Resource... contexts) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
        this.contexts = contexts;
    }

    @Override
    public int hashCode() {
        return 961 * ((this.getSubject() == null) ? (0) : (this.getSubject().hashCode())) +
                31 * ((this.getPredicate() == null) ? (0) : (this.getPredicate().hashCode())) +
                ((this.getObject() == null) ? (0) : (this.getObject().hashCode()));
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
        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other instanceof Statement) {
            Statement otherSt = (Statement) other;
            return this.hashCode() == otherSt.hashCode();
        } else {
            return false;
        }
    }

    public Value getObject() {
        return object;
    }

    public URI getPredicate() {
        return predicate;
    }

    public Resource getSubject() {
        return subject;
    }

    public Resource getContext() {
        if (contexts == null || contexts.length == 0)
            return null;
        else return contexts[0];
    }

    public Resource[] getContexts() {
        return contexts;
    }

    public void setContexts(Resource[] contexts) {
        this.contexts = contexts;
    }
}
