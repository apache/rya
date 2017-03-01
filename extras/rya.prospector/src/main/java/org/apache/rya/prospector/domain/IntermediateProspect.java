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
package org.apache.rya.prospector.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.rya.prospector.mr.Prospector;
import org.apache.rya.prospector.plans.IndexWorkPlan;

/**
 * Represents a piece of information that is being counted during the process
 * of running a {@link Prospector} job.
 */
public class IntermediateProspect implements WritableComparable<IntermediateProspect> {

    private String index;
    private String data;
    private String dataType;
    private TripleValueType tripleValueType;
    private String visibility;

    /**
     * Constructs an uninitialized instance of {@link IntermediateProspect}.
     * This constructor is required to integration with Map Reduce's
     * {@link WritableComparable} interface.
     */
    public IntermediateProspect() { }

    /**
     * Constructs an instance of {@link IntermediateProspect}.
     *
     * @param index - Indicates which {@link IndexWorkPlan} the data is part of.
     * @param data - The information that is being counted.
     * @param dataType - The data type of {@code data}.
     * @param tripleValueType - Indicates which parts of the RDF Statement are included in {@code data}.
     * @param visibility - The visibility of this entry.
     */
    public IntermediateProspect(
            final String index,
            final String data,
            final String dataType,
            final TripleValueType tripleValueType,
            final String visibility) {
        this.index = index;
        this.data = data;
        this.dataType = dataType;
        this.tripleValueType = tripleValueType;
        this.visibility = visibility;
    }

    /**
     * @return Indicates which {@link IndexWorkPlan} the data is part of.
     */
    public String getIndex() {
        return index;
    }

    /**
     * @return The information that is being counted.
     */
    public String getData() {
        return data;
    }

    /**
     * @return The data type of {@code data}.
     */
    public String getDataType() {
        return dataType;
    }

    /**
     * @return Indicates which parts of the RDF Statement are included in {@code data}.
     */
    public TripleValueType getTripleValueType() {
        return tripleValueType;
    }

    /**
     * @return The visibility of this entry.
     */
    public String getVisibility() {
        return visibility;
    }

    @Override
    public int compareTo(IntermediateProspect t) {
        if(!index.equals(t.index)) {
            return index.compareTo(t.index);
        }
        if(!data.equals(t.data)) {
            return data.compareTo(t.data);
        }
        if(!dataType.equals(t.dataType)) {
            return dataType.compareTo(t.dataType);
        }
        if(!tripleValueType.equals(t.tripleValueType)) {
            return tripleValueType.compareTo(t.tripleValueType);
        }
        if(!visibility.equals(t.visibility)) {
            return visibility.compareTo(t.visibility);
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(index);
        dataOutput.writeUTF(data);
        dataOutput.writeUTF(dataType);
        dataOutput.writeUTF(tripleValueType.name());
        dataOutput.writeUTF(visibility);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        index = dataInput.readUTF();
        data = dataInput.readUTF();
        dataType = dataInput.readUTF();
        tripleValueType = TripleValueType.valueOf(dataInput.readUTF());
        visibility = dataInput.readUTF();
    }

    /**
     * @return An empty instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builds instances of {@link IntermediateProspect}.
     */
    public static final class Builder {

        private String index;
        private String data;
        private String dataType;
        private TripleValueType tripleValueType;
        private String visibility;

        /**
         * @param index - Indicates which {@link IndexWorkPlan} the data is part of.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setIndex(String index) {
            this.index = index;
            return this;
        }

        /**
         * @param data - The information that is being counted.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setData(String data) {
            this.data = data;
            return this;
        }

        /**
         * @param dataType - The data type of {@code data}.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setDataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        /**
         * @param tripleValueType - Indicates which parts of the RDF Statement are included in {@code data}.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setTripleValueType(TripleValueType tripleValueType) {
            this.tripleValueType = tripleValueType;
            return this;
        }

        /**
         * @param visibility - The visibility of this entry.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setVisibility(String visibility) {
            this.visibility = visibility;
            return this;
        }

        /**
         * @return Constructs an instance of {@link IntermediateProspect} built using this builder's values.
         */
        public IntermediateProspect build() {
            return new IntermediateProspect(index, data, dataType, tripleValueType, visibility);
        }
    }
}