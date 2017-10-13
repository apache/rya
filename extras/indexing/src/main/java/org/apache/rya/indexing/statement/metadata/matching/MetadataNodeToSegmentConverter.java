package org.apache.rya.indexing.statement.metadata.matching;
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
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.rya.indexing.external.matching.ExternalSetConverter;
import org.apache.rya.indexing.external.matching.JoinSegment;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.ValueExpr;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class MetadataNodeToSegmentConverter implements ExternalSetConverter<StatementMetadataNode<?>> {

    @Override
    public QuerySegment<StatementMetadataNode<?>> setToSegment(final StatementMetadataNode<?> set) {
        final Set<QueryModelNode> patterns = Sets.newHashSet(set.getReifiedStatementPatterns());
        final List<QueryModelNode> patternList = Lists.newArrayList(patterns);
        return new JoinSegment<StatementMetadataNode<?>>(patterns, patternList, new HashMap<ValueExpr, Filter>());
    }

}
