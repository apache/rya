package org.apache.rya.indexing.pcj.fluo.app.batch;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;

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
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.rya.indexing.pcj.fluo.app.util.BindingHashShardingFunction;

/**
 * This class represents a batch order to delete all entries in the Fluo table indicated
 * by the given Span and Column.  These batch orders are processed by the {@link BatchObserver},
 * which uses this batch information along with the nodeId passed into the Observer to perform
 * batch deletes.
 *
 */
public class SpanBatchDeleteInformation extends AbstractSpanBatchInformation {

    private static final BatchBindingSetUpdater updater = new SpanBatchBindingSetUpdater();
    private Optional<String> nodeId;

    /**
     * Create a new SpanBatchInformation object.
     * @param nodeId - Optional nodeId that is used to filter returned results.  Useful if the shard Id
     * is not included in the Span (see {@link BindingHashShardingFunction} for more info about how sharded
     * row keys are generated).
     * @param batchSize - size of batch to be deleted
     * @param column - column whose entries will be deleted
     * @param span - Span indicating the range of data to delete.  Sometimes the Span cannot contain the hash
     * (for example, if you are deleting all of the results associated with a nodeId).  In this case, a nodeId
     * should be specified along with a Span equal to the prefix of the nodeId.
     * @throws IllegalArgumentException if nodeId, column or span is null and if batchSize <= 0.
     */
    public SpanBatchDeleteInformation(Optional<String> nodeId, int batchSize, Column column, Span span) {
        super(batchSize, Task.Delete, column, span);
        checkNotNull(nodeId);
        this.nodeId = nodeId;
    }

    /**
     * @return Updater that applies the {@link Task} to the given {@link Span} and {@link Column}
     */
    @Override
    public BatchBindingSetUpdater getBatchUpdater() {
        return updater;
    }

    /**
     * Returns an Optional nodeId.  If this value is specified, the results
     * returned from the Fluo scan over the indicated range will be filtered
     * by the nodeId.  The nodeId allows results for a given query nodeId to be
     * deleted using a Span even if the hash cannot be specified when forming the
     * rowId in the table.
     * @return - the nodeId whose results will be batch deleted
     */
    public Optional<String> getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Span Batch Information {\n")
                .append("    Span: " + super.getSpan() + "\n")
                .append("    Batch Size: " + super.getBatchSize() + "\n")
                .append("    Task: " + super.getTask() + "\n")
                .append("    Column: " + super.getColumn() + "\n")
                .append("    NodeId: " + nodeId + "\n")
                .append("}")
                .toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private int batchSize = DEFAULT_BATCH_SIZE;
        private Column column;
        private Span span;
        private Optional<String> nodeId = Optional.empty();

        /**
         * @param batchSize - {@link Task}s are applied in batches of this size
         */
        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets column to apply batch {@link Task} to
         * @param column - column batch Task will be applied to
         * @return
         */
        public Builder setColumn(Column column) {
            this.column = column;
            return this;
        }

        /**
         * @param span - span that batch {@link Task} will be applied to
         *
         */
        public Builder setSpan(Span span) {
            this.span = span;
            return this;
        }

        /**
         * Sets the nodeId whose results will be batch deleted.  This optional value
         * allows the {@link SpanBatchBindingSetUpdater} to filter on the indicated
         * nodeId.  Because the results of the Fluo table are sharded, if the Span does
         * not include the shard, then it is not possible to scan exactly for all results
         * pertaining to a specific nodeId.  In the event that a user wants to delete all nodes
         * related to a specific entry, this Optional nodeId should be specified to retrieve
         * only the results associated with the indicated nodeId.
         * @param nodeId - node whose results will be batch deleted
         * @return - Builder for chaining method calls
         */
        public Builder setNodeId(Optional<String> nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        /**
         * @return an instance of {@link SpanBatchDeleteInformation} constructed from parameters passed to this Builder
         */
        public SpanBatchDeleteInformation build() {
            return new SpanBatchDeleteInformation(nodeId, batchSize, column, span);
        }

    }


}
