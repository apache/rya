package org.apache.rya.accumulo.mr;

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

import org.apache.hadoop.io.WritableComparable;
import org.apache.rya.api.domain.RyaType;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class RyaTypeWritable implements WritableComparable<RyaTypeWritable>{

    private RyaType ryatype;

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

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        SimpleValueFactory vfi = SimpleValueFactory.getInstance();
        String data = dataInput.readLine();
        String dataTypeString = dataInput.readLine();
        IRI dataType = vfi.createIRI(dataTypeString);
        ryatype.setData(data);
        ryatype.setDataType(dataType);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(ryatype.getData());
        dataOutput.writeChars(ryatype.getDataType().toString());
    }

    /**
     * Gets the contained RyaStatement.
     * @return The statement represented by this RyaStatementWritable.
     */
    public RyaType getRyaType() {
        return ryatype;
    }
    /**
     * Sets the contained RyaStatement.
     * @param   ryaStatement    The statement to be represented by this
     *                          RyaStatementWritable.
     */
    public void setRyaType(RyaType ryatype) {
        this.ryatype = ryatype;
    }

    @Override
    public int compareTo(RyaTypeWritable o) {
        return ryatype.compareTo(o.ryatype);
    }

    /**
     * Tests for equality using the equals method of the enclosed RyaType.
     * @param   o   Object to compare with
     * @return  true if both objects are RyaTypeWritables containing equivalent
     *          RyaTypes.
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || !(o instanceof RyaTypeWritable)) {
            return false;
        }
        RyaType rtThis = ryatype;
        RyaType rtOther = ((RyaTypeWritable) o).ryatype;
        if (rtThis == null) {
            return rtOther == null;
        }
        else {
            return rtThis.equals(rtOther);
        }
    }

    @Override
    public int hashCode() {
        if (ryatype == null) {
            return 0;
        }
        else {
            return ryatype.hashCode();
        }
    }
}
