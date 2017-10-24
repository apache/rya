package org.apache.rya.indexing.pcj.fluo.app.batch;
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
import org.apache.rya.indexing.pcj.fluo.app.query.CommonNodeMetadataImpl;

/**
 * This class represents a batch order to delete all entries in the Fluo table indicated
 * by the given Span and Column.  These batch orders are processed by the {@link BatchObserver},
 * which uses this batch information along with the nodeId passed into the Observer to perform
 * batch deletes.  
 *
 */
public class SpanBatchDeleteInformation extends AbstractSpanBatchInformation {

    private static final BatchBindingSetUpdater updater = new SpanBatchBindingSetUpdater();
    
    public SpanBatchDeleteInformation(int batchSize, Column column, Span span) {
        super(batchSize, Task.Delete, column, span, Optional.empty());
    }
    
    public SpanBatchDeleteInformation(int batchSize, Column column, Span span, Optional<CommonNodeMetadataImpl> aggregationStateMeta) {
        super(batchSize, Task.Delete, column, span, aggregationStateMeta);
    }
    
    /**
     * @return Updater that applies the {@link Task} to the given {@link Span} and {@link Column}
     */
    @Override
    public BatchBindingSetUpdater getBatchUpdater() {
        return updater;
    }
    
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {

        private int batchSize = DEFAULT_BATCH_SIZE;
        private Column column;
        private Span span;
        private CommonNodeMetadataImpl aggregationStateMetadata;

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
         * @param aggregationStateMetadata - metadata for checking aggregation state before writing or deleting
         */
        public Builder setAggregationStateMetadata(CommonNodeMetadataImpl aggregationStateMetadata) {
            this.aggregationStateMetadata = aggregationStateMetadata;
            return this;
        }


        /**
         * @return an instance of {@link SpanBatchDeleteInformation} constructed from parameters passed to this Builder
         */
        public SpanBatchDeleteInformation build() {
            return new SpanBatchDeleteInformation(batchSize, column, span, Optional.ofNullable(aggregationStateMetadata));
        }

    }


}
