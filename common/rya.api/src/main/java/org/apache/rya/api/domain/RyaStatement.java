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
package org.apache.rya.api.domain;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Date: 7/17/12
 * Time: 7:20 AM
 */
public class RyaStatement {
    private RyaIRI subject;
    private RyaIRI predicate;
    private RyaType object;
    private RyaIRI context;
    private String qualifer;
    private byte[] columnVisibility;
    private byte[] value;
    private Long timestamp;

    public RyaStatement() {
    }

    public RyaStatement(final RyaIRI subject, final RyaIRI predicate, final RyaType object) {
        this(subject, predicate, object, null);
    }

    public RyaStatement(final RyaIRI subject, final RyaIRI predicate, final RyaType object, final RyaIRI context) {
        this(subject, predicate, object, context, null);
    }


    public RyaStatement(final RyaIRI subject, final RyaIRI predicate, final RyaType object, final RyaIRI context, final String qualifier) {
        this(subject, predicate, object, context, qualifier, new StatementMetadata());
    }

    public RyaStatement(final RyaIRI subject, final RyaIRI predicate, final RyaType object, final RyaIRI context, final String qualifier, final StatementMetadata metadata) {
        this(subject, predicate, object, context, qualifier, metadata, null);
    }

    public RyaStatement(final RyaIRI subject, final RyaIRI predicate, final RyaType object, final RyaIRI context, final String qualifier, final StatementMetadata metadata, final byte[] columnVisibility) {
        this(subject, predicate, object, context, qualifier, columnVisibility, metadata.toBytes());
    }

    @Deprecated
    public RyaStatement(final RyaIRI subject, final RyaIRI predicate, final RyaType object, final RyaIRI context, final String qualifier, final byte[] columnVisibility, final byte[] value) {
        this(subject, predicate, object, context, qualifier, columnVisibility, value, null);
    }

    @Deprecated
    public RyaStatement(final RyaIRI subject, final RyaIRI predicate, final RyaType object, final RyaIRI context, final String qualifier, final byte[] columnVisibility, final byte[] value, final Long timestamp) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
        this.context = context;
        this.qualifer = qualifier;
        this.columnVisibility = columnVisibility;
        this.value = value;
        this.timestamp = timestamp != null ? timestamp : System.currentTimeMillis();
    }

    public RyaIRI getSubject() {
        return subject;
    }

    public void setSubject(final RyaIRI subject) {
        this.subject = subject;
    }

    public RyaIRI getPredicate() {
        return predicate;
    }

    public void setPredicate(final RyaIRI predicate) {
        this.predicate = predicate;
    }

    public RyaType getObject() {
        return object;
    }

    public void setObject(final RyaType object) {
        this.object = object;
    }

    public RyaIRI getContext() {
        return context;
    }

    public void setContext(final RyaIRI context) {
        this.context = context;
    }

    public byte[] getColumnVisibility() {
        return columnVisibility;
    }

    public void setColumnVisibility(final byte[] columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    public StatementMetadata getMetadata() {
        // try to deserialize the value, if not assume that there was
        // no explicit metadata
        try {
            return new StatementMetadata(value);
        }
        catch (final Exception ex){
            return null;
        }
    }

    public void setStatementMetadata(final StatementMetadata metadata){
        this.value = metadata.toBytes();
    }

    @Deprecated
    public byte[] getValue() {
        return value;
    }

    @Deprecated
    public void setValue(final byte[] value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final RyaStatement that = (RyaStatement) o;

        if (!Arrays.equals(columnVisibility, that.columnVisibility)) {
            return false;
        }
        if (context != null ? !context.equals(that.context) : that.context != null) {
            return false;
        }
        if (object != null ? !object.equals(that.object) : that.object != null) {
            return false;
        }
        if (predicate != null ? !predicate.equals(that.predicate) : that.predicate != null) {
            return false;
        }
        if (qualifer != null ? !qualifer.equals(that.qualifer) : that.qualifer != null) {
            return false;
        }
        if (subject != null ? !subject.equals(that.subject) : that.subject != null) {
            return false;
        }
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) {
            return false;
        }
        if (!Arrays.equals(value, that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = subject != null ? subject.hashCode() : 0;
        result = 31 * result + (predicate != null ? predicate.hashCode() : 0);
        result = 31 * result + (object != null ? object.hashCode() : 0);
        result = 31 * result + (context != null ? context.hashCode() : 0);
        result = 31 * result + (qualifer != null ? qualifer.hashCode() : 0);
        result = 31 * result + (columnVisibility != null ? Arrays.hashCode(columnVisibility) : 0);
        result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }

    public String getQualifer() {
        return qualifer;
    }

    public void setQualifer(final String qualifer) {
        this.qualifer = qualifer;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("RyaStatement");
        sb.append("{subject=").append(subject);
        sb.append(", predicate=").append(predicate);
        sb.append(", object=").append(object);
        sb.append(", context=").append(context);
        sb.append(", qualifier=").append(qualifer);
        sb.append(", columnVisibility=").append(columnVisibility == null ? "null" : new String(columnVisibility, StandardCharsets.UTF_8));
        sb.append(", value=").append(value == null ? "null" : new String(value, StandardCharsets.UTF_8));
        sb.append(", timestamp=").append(timestamp);
        sb.append('}');
        return sb.toString();
    }

    public static RyaStatementBuilder builder() {
        return new RyaStatementBuilder();
    }

    public static RyaStatementBuilder builder(final RyaStatement ryaStatement) {
        return new RyaStatementBuilder(ryaStatement);
    }


    //builder
    public static class RyaStatementBuilder {

        RyaStatement ryaStatement;

        public RyaStatementBuilder() {
            ryaStatement = new RyaStatement();
        }

        public RyaStatementBuilder(final RyaStatement ryaStatement) {
            this.ryaStatement = ryaStatement;
        }

        public RyaStatementBuilder setTimestamp(final Long timestamp) {
            ryaStatement.setTimestamp(timestamp);
            return this;
        }

        @Deprecated
        public RyaStatementBuilder setValue(final byte[] value) {
            ryaStatement.setValue(value);
            return this;
        }

        public RyaStatementBuilder setMetadata(final StatementMetadata metadata) {
            ryaStatement.setValue(metadata.toBytes());
            return this;
        }

        public RyaStatementBuilder setColumnVisibility(final byte[] columnVisibility) {
            ryaStatement.setColumnVisibility(columnVisibility);
            return this;
        }

        public RyaStatementBuilder setQualifier(final String str) {
            ryaStatement.setQualifer(str);
            return this;
        }

        public RyaStatementBuilder setContext(final RyaIRI ryaIRI) {
            ryaStatement.setContext(ryaIRI);
            return this;
        }

        public RyaStatementBuilder setSubject(final RyaIRI ryaIRI) {
            ryaStatement.setSubject(ryaIRI);
            return this;
        }

        public RyaStatementBuilder setPredicate(final RyaIRI ryaIRI) {
            ryaStatement.setPredicate(ryaIRI);
            return this;
        }

        public RyaStatementBuilder setObject(final RyaType ryaType) {
            ryaStatement.setObject(ryaType);
            return this;
        }

        public RyaStatement build() {
            return ryaStatement;
        }
    }
}
