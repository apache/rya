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

import static org.apache.rya.mongodb.document.operators.aggregation.PipelineOperators.redact;
import static org.apache.rya.mongodb.document.operators.aggregation.SetOperators.anyElementTrue;
import static org.apache.rya.mongodb.document.operators.aggregation.SetOperators.setIsSubsetNullSafe;
import static org.apache.rya.mongodb.document.operators.aggregation.VariableOperators.map;
import static org.apache.rya.mongodb.document.operators.query.ArrayOperators.size;
import static org.apache.rya.mongodb.document.operators.query.ComparisonOperators.eq;
import static org.apache.rya.mongodb.document.operators.query.LogicalOperators.or;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.mongodb.MongoDbRdfConstants;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.apache.rya.mongodb.document.operators.aggregation.PipelineOperators.RedactAggregationResult;
import org.apache.rya.mongodb.document.util.AuthorizationsUtil;
import org.bson.Document;

import com.google.common.collect.Lists;
import com.mongodb.DBObject;

/**
 * Utility methods for MongoDB aggregation.
 */
public final class AggregationUtil {
    /**
     * Private constructor to prevent instantiation.
     */
    private AggregationUtil() {
    }

    /**
     * Creates a MongoDB $redact aggregation pipeline that only include
     * documents whose document visibility match the provided authorizations.
     * All other documents are excluded.
     * @param authorizations the {@link Authorization}s to include in the
     * $redact. Only documents that match the authorizations will be returned.
     * @return the {@link List} of {@link DBObject}s that represents the $redact
     * aggregation pipeline.
     */
    public static List<Document> createRedactPipeline(final Authorizations authorizations) {
        if (MongoDbRdfConstants.ALL_AUTHORIZATIONS.equals(authorizations)) {
            return Lists.newArrayList();
        }

        final List<String> authList = AuthorizationsUtil.getAuthorizationsStrings(authorizations);

        final String documentVisibilityField = "$" + SimpleMongoDBStorageStrategy.DOCUMENT_VISIBILITY;

        final String mapVariableCursorName = "dvItemCursorTag";

        final Document anyElementTrue =
            anyElementTrue(
                map(
                    documentVisibilityField,
                    mapVariableCursorName,
                    setIsSubsetNullSafe(
                        "$$" + mapVariableCursorName,
                        authList
                    )
                )
            );

        // If the field is empty then there are no authorizations required,
        // so all users should be able to view it when they query.
        final Document isFieldSizeZero =
            eq(
                size(documentVisibilityField),
                0
            );

        final Document orExpression = or(anyElementTrue, isFieldSizeZero);

        final List<Document> pipeline = new ArrayList<>();
        pipeline.add(
            redact(
                orExpression,
                RedactAggregationResult.DESCEND,
                RedactAggregationResult.PRUNE
            )
        );

        return pipeline;
    }
}