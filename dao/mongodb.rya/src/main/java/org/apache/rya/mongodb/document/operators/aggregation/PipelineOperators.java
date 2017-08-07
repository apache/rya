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
package org.apache.rya.mongodb.document.operators.aggregation;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.mongodb.document.operators.query.ConditionalOperators.cond;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;

/**
 * Utility methods for pipeline operators.
 */
public final class PipelineOperators {
    /**
     * The result that the $redact aggregation resolves to.
     */
    public static enum RedactAggregationResult {
        /**
         * $redact returns the fields at the current document level, excluding
         * embedded documents. To include embedded documents and embedded
         * documents within arrays, apply the $cond expression to the embedded
         * documents to determine access for these embedded documents.
         */
        DESCEND("$$DESCEND"),
        /**
         * $redact excludes all fields at this current document/embedded
         * document level, without further inspection of any of the excluded
         * fields. This applies even if the excluded field contains embedded
         * documents that may have different access levels.
         */
        PRUNE("$$PRUNE"),
        /**
         *  $redact returns or keeps all fields at this current
         *  document/embedded document level, without further inspection of the
         *  fields at this level. This applies even if the included field
         *  contains embedded documents that may have different access levels.
         */
        KEEP("$$KEEP");

        private final String resultName;

        /**
         * Creates a new {@link RedactAggregationResult}.
         * @param resultName the name of the redact aggregation result.
         */
        private RedactAggregationResult(final String resultName) {
            this.resultName = resultName;
        }

        @Override
        public String toString() {
            return resultName;
        }
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private PipelineOperators() {
    }

    /**
     * Creates a $redact expression.
     * @param expression the expression to run redact on.
     * @param acceptResult the {@link RedactAggregationResult} to return when
     * the expression passes.
     * @param rejectResult the {@link RedactAggregationResult} to return when
     * the expression fails.
     * @return the $redact expression {@link BasicDBObjectBuilder}.
     */
    public static Document redact(final Document expression, final RedactAggregationResult acceptResult, final RedactAggregationResult rejectResult) {
        return new Document("$redact", cond(expression, acceptResult.toString(), rejectResult.toString()));
    }
}