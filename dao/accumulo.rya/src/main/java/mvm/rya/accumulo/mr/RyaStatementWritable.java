package mvm.rya.accumulo.mr;

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



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RyaTripleContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;

/**
 * Date: 7/17/12
 * Time: 1:29 PM
 */
public class RyaStatementWritable implements WritableComparable {

    private RyaTripleContext ryaContext;
    private RyaStatement ryaStatement;
    
    public RyaStatementWritable(Configuration conf) {
        this();
    }
     
    public RyaStatementWritable(RyaTripleContext ryaContext) {
     	this.ryaContext = ryaContext;
    }
    
    public RyaStatementWritable() {
        this.ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration());
    }
    
    public RyaStatementWritable(RyaStatement ryaStatement, RyaTripleContext ryaContext) {
    	this(ryaContext);
        this.ryaStatement = ryaStatement;
    }

    public RyaStatement getRyaStatement() {
        return ryaStatement;
    }

    public void setRyaStatement(RyaStatement ryaStatement) {
        this.ryaStatement = ryaStatement;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof RyaStatementWritable) {
            return (getRyaStatement().equals(((RyaStatementWritable) o).getRyaStatement())) ? (0) : (-1);
        }
        return -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (ryaStatement == null) {
            throw new IOException("Rya Statement is null");
        }
        try {
            Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> map = ryaContext.serializeTriple(ryaStatement);
            TripleRow tripleRow = map.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
            byte[] row = tripleRow.getRow();
            byte[] columnFamily = tripleRow.getColumnFamily();
            byte[] columnQualifier = tripleRow.getColumnQualifier();
            write(dataOutput, row);
            write(dataOutput, columnFamily);
            write(dataOutput, columnQualifier);
            write(dataOutput, ryaStatement.getColumnVisibility());
            write(dataOutput, ryaStatement.getValue());
            Long timestamp = ryaStatement.getTimestamp();
            boolean b = timestamp != null;
            dataOutput.writeBoolean(b);
            if (b) {
                dataOutput.writeLong(timestamp);
            }
        } catch (TripleRowResolverException e) {
            throw new IOException(e);
        }
    }

    protected void write(DataOutput dataOutput, byte[] row) throws IOException {
        boolean b = row != null;
        dataOutput.writeBoolean(b);
        if (b) {
            dataOutput.writeInt(row.length);
            dataOutput.write(row);
        }
    }

    protected byte[] read(DataInput dataInput) throws IOException {
        if (dataInput.readBoolean()) {
            int len = dataInput.readInt();
            byte[] bytes = new byte[len];
            dataInput.readFully(bytes);
            return bytes;
        }else {
            return null;
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        byte[] row = read(dataInput);
        byte[] columnFamily = read(dataInput);
        byte[] columnQualifier = read(dataInput);
        byte[] columnVisibility = read(dataInput);
        byte[] value = read(dataInput);
        boolean b = dataInput.readBoolean();
        Long timestamp = null;
        if (b) {
            timestamp = dataInput.readLong();
        }
        try {
            ryaStatement = ryaContext.deserializeTriple(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO,
                    new TripleRow(row, columnFamily, columnQualifier));
            ryaStatement.setColumnVisibility(columnVisibility);
            ryaStatement.setValue(value);
            ryaStatement.setTimestamp(timestamp);
        } catch (TripleRowResolverException e) {
            throw new IOException(e);
        }
    }
}
