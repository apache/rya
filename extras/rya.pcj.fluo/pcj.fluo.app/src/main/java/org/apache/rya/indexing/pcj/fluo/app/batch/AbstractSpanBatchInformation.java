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
package org.apache.rya.indexing.pcj.fluo.app.batch;

import java.util.Objects;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;

import com.google.common.base.Preconditions;

/**
 * Abstract class for generating span based notifications.  A spanned notification
 * uses a {@link Span} to begin processing a Fluo Column at the position designated by the Span.
 *
 */
public abstract class AbstractSpanBatchInformation extends BasicBatchInformation {

    private Span span;

    /**
     * Create AbstractBatchInformation
     * @param batchSize - size of batch to be processed
     * @param task - type of task processed (Add, Delete, Udpate)
     * @param column - Cpolumn that Span notification is applied
     * @param span - span used to indicate where processing should begin
     */
    public AbstractSpanBatchInformation(int batchSize, Task task, Column column, Span span) {
        super(batchSize, task, column);
        this.span = Preconditions.checkNotNull(span);
    }

    public AbstractSpanBatchInformation(Task task, Column column, Span span) {
        this(DEFAULT_BATCH_SIZE, task, column, span);
    }

    /**
     * @return Span that batch Task will be applied to
     */
    public Span getSpan() {
        return span;
    }

    /**
     * Sets span to which batch Task will be applied
     * @param span
     */
    public void setSpan(Span span) {
        this.span = span;
    }
    
    @Override
    public String toString() {
        return new StringBuilder()
                .append("Span Batch Information {\n")
                .append("    Span: " + span + "\n")
                .append("    Batch Size: " + super.getBatchSize() + "\n")
                .append("    Task: " + super.getTask() + "\n")
                .append("    Column: " + super.getColumn() + "\n")
                .append("}")
                .toString();
    }
    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof AbstractSpanBatchInformation)) {
            return false;
        }

        AbstractSpanBatchInformation batch = (AbstractSpanBatchInformation) other;
        return (super.getBatchSize() == batch.getBatchSize()) && Objects.equals(super.getColumn(), batch.getColumn()) && Objects.equals(this.span, batch.span)
                && Objects.equals(super.getTask(), batch.getTask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.getBatchSize(), span, super.getColumn(), super.getTask());
    }
    

}
