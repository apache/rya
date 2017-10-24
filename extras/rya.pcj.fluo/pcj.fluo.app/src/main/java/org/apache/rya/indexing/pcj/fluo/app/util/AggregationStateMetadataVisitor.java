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
package org.apache.rya.indexing.pcj.fluo.app.util;


import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rya.indexing.pcj.fluo.app.query.CommonNodeMetadataImpl;
import org.apache.rya.indexing.pcj.fluo.app.query.ConstructQueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FilterMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.ProjectionMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;

/**
 *  This builder visitor inserts the aggregation metadata into all of its
 *  ancestors.
 */
public class AggregationStateMetadataVisitor extends StopNodeVisitor {

    private CommonNodeMetadataImpl aggStateMeta;

    public AggregationStateMetadataVisitor(FluoQuery.Builder fluoQueryBuilder, String stopNodeId, CommonNodeMetadataImpl aggStateMeta) {
        super(fluoQueryBuilder, stopNodeId);
        this.aggStateMeta = checkNotNull(aggStateMeta);
    }

    @Override
    public void visit(QueryMetadata.Builder builder) {
        builder.setStateMetadata(new CommonNodeMetadataImpl(aggStateMeta));
        super.visit(builder);
    }

    @Override
    public void visit(ProjectionMetadata.Builder builder) {
        builder.setStateMetadata(new CommonNodeMetadataImpl(aggStateMeta));
        super.visit(builder);
    }

    @Override
    public void visit(ConstructQueryMetadata.Builder builder) {
        builder.setStateMetadata(new CommonNodeMetadataImpl(aggStateMeta));
        super.visit(builder);
    }

    @Override
    public void visit(FilterMetadata.Builder builder) {
        builder.setStateMetadata(new CommonNodeMetadataImpl(aggStateMeta));
        super.visit(builder);
    }

    @Override
    public void visit(PeriodicQueryMetadata.Builder builder) {
        builder.setStateMetadata(new CommonNodeMetadataImpl(aggStateMeta));
        super.visit(builder);
    }

    @Override
    public void visit(JoinMetadata.Builder builder) {
        builder.setStateMetadata(new CommonNodeMetadataImpl(aggStateMeta));
        super.visit(builder);
    }
}
