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

import java.util.Objects;

import org.apache.rya.prospector.mr.Prospector;
import org.apache.rya.prospector.plans.IndexWorkPlan;

/**
 * Represents a count that was the result of a {@link Prospector} run.
 */
public class IndexEntry {

    private final String index;
    private final String data;
    private final String dataType;
    private final String tripleValueType;
    private final String visibility;
    private final Long count;
    private final Long timestamp;

    /**
     * Constructs an instance of {@link IndexEntry}.
     *
     * @param index - Indicates which {@link IndexWorkPlan} the data came from.
     * @param data - The information that is being counted.
     * @param dataType - The data type of {@code data}.
     * @param tripleValueType - Indicates which parts of the RDF Statement are included in {@code data}.
     * @param visibility - The visibility of this entry.
     * @param count - The number of times the {@code data} appeared within Rya.
     * @param timestamp - Identifies which Prospect run this entry belongs to.
     */
    public IndexEntry(
            final String index,
            final String data,
            final String dataType,
            final String tripleValueType,
            final String visibility,
            final Long count,
            final Long timestamp) {
        this.index = index;
        this.data = data;
        this.dataType = dataType;
        this.tripleValueType = tripleValueType;
        this.visibility = visibility;
        this.count = count;
        this.timestamp = timestamp;
    }

    /**
     * @return Indicates which {@link IndexWorkPlan} the data came from.
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
    public String getTripleValueType() {
        return tripleValueType;
    }

    /**
     * @return The visibility of this entry.
     */
    public String getVisibility() {
        return visibility;
    }

    /**
     * @return The number of times the {@code data} appeared within Rya.
     */
    public Long getCount() {
        return count;
    }

    /**
     * @return Identifies which Prospect run this entry belongs to.
     */
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "IndexEntry{" +
                "index='" + index + '\'' +
                ", data='" + data + '\'' +
                ", dataType='" + dataType + '\'' +
                ", tripleValueType=" + tripleValueType +
                ", visibility='" + visibility + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", count=" + count +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, data, dataType, tripleValueType, visibility, count, timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        }
        if(o instanceof IndexEntry) {
            final IndexEntry entry = (IndexEntry) o;
            return Objects.equals(index, entry.index) &&
                    Objects.equals(data, entry.data) &&
                    Objects.equals(dataType, entry.dataType) &&
                    Objects.equals(tripleValueType, entry.tripleValueType) &&
                    Objects.equals(visibility, entry.visibility) &&
                    Objects.equals(count, entry.count) &&
                    Objects.equals(timestamp, entry.timestamp);
        }
        return false;
    }

    /**
     * @return An empty instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builds instances of {@link IndexEntry}.
     */
    public static final class Builder {
        private String index;
        private String data;
        private String dataType;
        private String tripleValueType;
        private String visibility;
        private Long count;
        private Long timestamp;

        /**
         * @param index - Indicates which {@link IndexWorkPlan} the data came from.
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
        public Builder setTripleValueType(String tripleValueType) {
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
         * @param count - The number of times the {@code data} appeared within Rya.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setCount(Long count) {
            this.count = count;
            return this;
        }

        /**
         * @param timestamp - Identifies which Prospect run this entry belongs to.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        /**
         * @return Constructs an instance of {@link IndexEntry} built using this builder's values.
         */
        public IndexEntry build() {
            return new IndexEntry(index, data, dataType, tripleValueType, visibility, count, timestamp);
        }
    }
}