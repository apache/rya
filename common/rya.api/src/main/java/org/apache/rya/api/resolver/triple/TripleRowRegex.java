package org.apache.rya.api.resolver.triple;

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
 * Date: 7/13/12
 * Time: 8:54 AM
 */
public class TripleRowRegex {
    private String row, columnFamily, columnQualifier;

    public TripleRowRegex(String row, String columnFamily, String columnQualifier) {
        this.row = row;
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
    }

    public String getRow() {
        return row;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String getColumnQualifier() {
        return columnQualifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TripleRowRegex that = (TripleRowRegex) o;

        if (columnFamily != null ? !columnFamily.equals(that.columnFamily) : that.columnFamily != null) return false;
        if (columnQualifier != null ? !columnQualifier.equals(that.columnQualifier) : that.columnQualifier != null)
            return false;
        if (row != null ? !row.equals(that.row) : that.row != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = row != null ? row.hashCode() : 0;
        result = 31 * result + (columnFamily != null ? columnFamily.hashCode() : 0);
        result = 31 * result + (columnQualifier != null ? columnQualifier.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TripleRowRegex");
        sb.append("{row='").append(row).append('\'');
        sb.append(", columnFamily='").append(columnFamily).append('\'');
        sb.append(", columnQualifier='").append(columnQualifier).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
