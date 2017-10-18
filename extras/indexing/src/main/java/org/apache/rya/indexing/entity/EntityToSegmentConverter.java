/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.entity;

import java.util.*;

import com.google.common.base.Preconditions;
import org.apache.rya.indexing.entity.query.EntityQueryNode;
import org.apache.rya.indexing.external.matching.ExternalSetConverter;
import org.apache.rya.indexing.external.matching.JoinSegment;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

/**
 * Implementation of {@link ExternalSetConverter} to convert {@link EntityQueryNode}s
 * to {@link QuerySegment}s.
 *
 */
public class EntityToSegmentConverter implements ExternalSetConverter<EntityQueryNode> {

    @Override
    public QuerySegment<EntityQueryNode> setToSegment(final EntityQueryNode set) {
        Preconditions.checkNotNull(set);
        final Set<QueryModelNode> matched = new HashSet<>(set.getPatterns());
        final List<QueryModelNode> unmatched = new ArrayList<>(set.getPatterns());
        return new JoinSegment<EntityQueryNode>(matched, unmatched, new HashMap<ValueExpr, Filter>());
    }
}
