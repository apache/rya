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

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RyaTripleContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;

/**
 * Basic {@link WritableComparable} for using Rya data with Hadoop.
 * RyaStatementWritable wraps a {@link RyaStatement}, which in turn represents a
 * statement as  a collection of {@link mvm.rya.api.domain.RyaURI} and
 * {@link mvm.rya.api.domain.RyaType} objects.
 * <p>
 * This class is mutable, like all {@link org.apache.hadoop.io.Writable}s. When
 * used as Mapper or Reducer input, the Hadoop framework will typically reuse
 * the same object to load the next record. However, loading the next record
 * will create a new RyaStatement internally. Therefore, if a statement must be
 * stored for any length of time, be sure to extract the internal RyaStatement.
 */
public class RyaStatementWritable implements WritableComparable<RyaStatementWritable> {
    private RyaTripleContext ryaContext;
    private RyaStatement ryaStatement;

    /**
     * Instantiates a RyaStatementWritable with the default RyaTripleContext.
     * @param conf  Unused.
     */
    public RyaStatementWritable(Configuration conf) {
        this();
    }
    /**
     * Instantiates a RyaStatementWritable with a given context.
     * @param ryaContext    Context used for reading and writing the statement.
     */
    public RyaStatementWritable(RyaTripleContext ryaContext) {
        this.ryaContext = ryaContext;
    }
    /**
     * Instantiates a RyaStatementWritable with the default RyaTripleContext.
     */
    public RyaStatementWritable() {
        this.ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration());
    }
    /**
     * Instantiates a RyaStatementWritable with a given statement and context.
     * @param ryaStatement  The statement (triple) represented by this object.
     * @param ryaContext    Context used for reading and writing the statement.
     */
    public RyaStatementWritable(RyaStatement ryaStatement, RyaTripleContext ryaContext) {
        this(ryaContext);
        this.ryaStatement = ryaStatement;
    }

    /**
     * Gets the contained RyaStatement.
     * @return The statement represented by this RyaStatementWritable.
     */
    public RyaStatement getRyaStatement() {
        return ryaStatement;
    }
    /**
     * Sets the contained RyaStatement.
     * @param   ryaStatement    The statement to be represented by this
     *                          RyaStatementWritable.
     */
    public void setRyaStatement(RyaStatement ryaStatement) {
        this.ryaStatement = ryaStatement;
    }

    /**
     * Comparison method for natural ordering. Compares based on the logical
     * triple (the s/p/o/context information in the underlying RyaStatement)
     * and then by the metadata contained in the RyaStatement if the triples are
     * the same.
     * @return  Zero if both RyaStatementWritables contain equivalent statements
     *          or both have null statements; otherwise, an integer whose sign
     *          corresponds to a consistent ordering.
     */
    @Override
    public int compareTo(RyaStatementWritable other) {
        CompareToBuilder builder = new CompareToBuilder();
        RyaStatement rsThis = this.getRyaStatement();
        RyaStatement rsOther = other.getRyaStatement(); // should throw NPE if other is null, as per Comparable contract
        builder.append(rsThis == null, rsOther == null);
        if (rsThis != null && rsOther != null) {
            builder.append(rsThis.getSubject(), rsOther.getSubject());
            builder.append(rsThis.getPredicate(), rsOther.getPredicate());
            builder.append(rsThis.getObject(), rsOther.getObject());
            builder.append(rsThis.getContext(), rsOther.getContext());
            builder.append(rsThis.getQualifer(), rsOther.getQualifer());
            builder.append(rsThis.getColumnVisibility(), rsOther.getColumnVisibility());
            builder.append(rsThis.getValue(), rsOther.getValue());
            builder.append(rsThis.getTimestamp(), rsOther.getTimestamp());
        }
        return builder.toComparison();
    }

    /**
     * Returns a hash based on the hashCode method in RyaStatement.
     * @return  A hash that should be consistent for equivalent RyaStatements.
     */
    @Override
    public int hashCode() {
        if (ryaStatement == null) {
            return 0;
        }
        return ryaStatement.hashCode();
    }

    /**
     * Tests for equality using the equals method in RyaStatement.
     * @param   o   Object to compare with
     * @return  true if both objects are RyaStatementWritables containing
     *          equivalent RyaStatements.
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || !(o instanceof RyaStatementWritable)) {
            return false;
        }
        RyaStatement rsThis = this.getRyaStatement();
        RyaStatement rsOther = ((RyaStatementWritable) o).getRyaStatement();
        if (rsThis == null) {
            return rsOther == null;
        }
        else {
            return rsThis.equals(rsOther);
        }
    }

    /**
     * Serializes this RyaStatementWritable.
     * @param   dataOutput  An output stream for serialized statement data.
     * @throws  IOException if the RyaStatement is null or otherwise can't be
     *          serialized.
     */
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

    /**
     * Write part of a statement to an output stream.
     * @param dataOutput Stream for writing serialized statements.
     * @param row   Individual field to write, as a byte array.
     * @throws IOException if writing to the stream fails.
     */
    protected void write(DataOutput dataOutput, byte[] row) throws IOException {
        boolean b = row != null;
        dataOutput.writeBoolean(b);
        if (b) {
            dataOutput.writeInt(row.length);
            dataOutput.write(row);
        }
    }

    /**
     * Read part of a statement from an input stream.
     * @param dataInput Stream for reading serialized statements.
     * @return The next individual field, as a byte array.
     * @throws IOException if reading from the stream fails.
     */
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

    /**
     * Loads a RyaStatementWritable by reading data from an input stream.
     * Creates a new RyaStatement and assigns it to this RyaStatementWritable.
     * @param   dataInput   An stream containing serialized statement data.
     */
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
