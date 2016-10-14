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



import java.util.Arrays;

/**
 * Date: 7/17/12
 * Time: 7:20 AM
 */
public class RyaStatement {
    private RyaURI subject;
    private RyaURI predicate;
    private RyaType object;
    private RyaURI context;
    private String qualifer;
    private byte[] columnVisibility;
    private byte[] value;
    private Long timestamp;

    public RyaStatement() {
    }

    public RyaStatement(RyaURI subject, RyaURI predicate, RyaType object) {
        this(subject, predicate, object, null);
    }

    public RyaStatement(RyaURI subject, RyaURI predicate, RyaType object, RyaURI context) {
        this(subject, predicate, object, context, null);
    }

    public RyaStatement(RyaURI subject, RyaURI predicate, RyaType object, RyaURI context, String qualifier) {
        this(subject, predicate, object, context, qualifier, null);
    }

    public RyaStatement(RyaURI subject, RyaURI predicate, RyaType object, RyaURI context, String qualifier, byte[] columnVisibility) {
        this(subject, predicate, object, context, qualifier, columnVisibility, null);
    }

    public RyaStatement(RyaURI subject, RyaURI predicate, RyaType object, RyaURI context, String qualifier, byte[] columnVisibility, byte[] value) {
        this(subject, predicate, object, context, qualifier, columnVisibility, value, null);
    }

    public RyaStatement(RyaURI subject, RyaURI predicate, RyaType object, RyaURI context, String qualifier, byte[] columnVisibility, byte[] value, Long timestamp) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
        this.context = context;
        this.qualifer = qualifier;
        this.columnVisibility = columnVisibility;
        this.value = value;
        this.timestamp = timestamp != null ? timestamp : System.currentTimeMillis();
    }

    public RyaURI getSubject() {
        return subject;
    }

    public void setSubject(RyaURI subject) {
        this.subject = subject;
    }

    public RyaURI getPredicate() {
        return predicate;
    }

    public void setPredicate(RyaURI predicate) {
        this.predicate = predicate;
    }

    public RyaType getObject() {
        return object;
    }

    public void setObject(RyaType object) {
        this.object = object;
    }

    public RyaURI getContext() {
        return context;
    }

    public void setContext(RyaURI context) {
        this.context = context;
    }

    public byte[] getColumnVisibility() {
        return columnVisibility;
    }

    public void setColumnVisibility(byte[] columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RyaStatement that = (RyaStatement) o;

        if (!Arrays.equals(columnVisibility, that.columnVisibility)) return false;
        if (context != null ? !context.equals(that.context) : that.context != null) return false;
        if (object != null ? !object.equals(that.object) : that.object != null) return false;
        if (predicate != null ? !predicate.equals(that.predicate) : that.predicate != null) return false;
        if (qualifer != null ? !qualifer.equals(that.qualifer) : that.qualifer != null) return false;
        if (subject != null ? !subject.equals(that.subject) : that.subject != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        if (!Arrays.equals(value, that.value)) return false;

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

    public void setQualifer(String qualifer) {
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
        sb.append(", columnVisibility=").append(columnVisibility == null ? "null" : new String(columnVisibility));
        sb.append(", value=").append(value == null ? "null" : new String(value));
        sb.append(", timestamp=").append(timestamp);
        sb.append('}');
        return sb.toString();
    }

    public static RyaStatementBuilder builder() {
        return new RyaStatementBuilder();
    }

    public static RyaStatementBuilder builder(RyaStatement ryaStatement) {
        return new RyaStatementBuilder(ryaStatement);
    }


    //builder
    public static class RyaStatementBuilder {

        RyaStatement ryaStatement;

        public RyaStatementBuilder() {
            ryaStatement = new RyaStatement();
        }

        public RyaStatementBuilder(RyaStatement ryaStatement) {
            this.ryaStatement = ryaStatement;
        }

        public RyaStatementBuilder setTimestamp(Long timestamp) {
            ryaStatement.setTimestamp(timestamp);
            return this;
        }

        public RyaStatementBuilder setValue(byte[] value) {
            ryaStatement.setValue(value);
            return this;
        }

        public RyaStatementBuilder setColumnVisibility(byte[] columnVisibility) {
            ryaStatement.setColumnVisibility(columnVisibility);
            return this;
        }

        public RyaStatementBuilder setQualifier(String str) {
            ryaStatement.setQualifer(str);
            return this;
        }

        public RyaStatementBuilder setContext(RyaURI ryaURI) {
            ryaStatement.setContext(ryaURI);
            return this;
        }

        public RyaStatementBuilder setSubject(RyaURI ryaURI) {
            ryaStatement.setSubject(ryaURI);
            return this;
        }

        public RyaStatementBuilder setPredicate(RyaURI ryaURI) {
            ryaStatement.setPredicate(ryaURI);
            return this;
        }

        public RyaStatementBuilder setObject(RyaType ryaType) {
            ryaStatement.setObject(ryaType);
            return this;
        }

        public RyaStatement build() {
            return ryaStatement;
        }
    }
}
