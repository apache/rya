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
public class TripleRow {
    private byte[] row, columnFamily, columnQualifier, columnVisibility, value;
    private Long timestamp;

    public TripleRow(byte[] row, byte[] columnFamily, byte[] columnQualifier) {
        this(row, columnFamily, columnQualifier, null, null, null);
    }
    public TripleRow(byte[] row, byte[] columnFamily, byte[] columnQualifier, Long timestamp,
                     byte[] columnVisibility, byte[] value) {
        this.row = row;
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        //Default TS to current time to ensure the timestamps on all the tables are the same for the same triple
        this.timestamp = timestamp != null ? timestamp : System.currentTimeMillis();
        this.columnVisibility = columnVisibility;
        this.value = value;
    }

    public byte[] getRow() {
        return row;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public byte[] getColumnQualifier() {
        return columnQualifier;
    }

    public byte[] getColumnVisibility() {
        return columnVisibility;
    }

    public byte[] getValue() {
        return value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TripleRow tripleRow = (TripleRow) o;

        if (!Arrays.equals(columnFamily, tripleRow.columnFamily)) return false;
        if (!Arrays.equals(columnQualifier, tripleRow.columnQualifier)) return false;
        if (!Arrays.equals(row, tripleRow.row)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = row != null ? Arrays.hashCode(row) : 0;
        result = 31 * result + (columnFamily != null ? Arrays.hashCode(columnFamily) : 0);
        result = 31 * result + (columnQualifier != null ? Arrays.hashCode(columnQualifier) : 0);
        result = 31 * result + (columnVisibility != null ? Arrays.hashCode(columnVisibility) : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TripleRow{" +
                "row=" + row +
                ", columnFamily=" + columnFamily +
                ", columnQualifier=" + columnQualifier +
                ", columnVisibility=" + columnVisibility +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
