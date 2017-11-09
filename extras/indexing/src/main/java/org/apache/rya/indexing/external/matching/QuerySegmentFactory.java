/*
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
package org.apache.rya.indexing.external.matching;

import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;

import com.google.common.base.Preconditions;

/**
 * Factory class for producing {@link QuerySegment}s from {@link QueryModelNode}s.
 *
 * @param <T> - ExternalSet parameter
 */
public class QuerySegmentFactory<T extends ExternalSet> {

    public QuerySegment<T> getQuerySegment(final QueryModelNode node) {
        Preconditions.checkNotNull(node);
        if(node instanceof Filter) {
            final Filter filter = (Filter)node;
            if(MatcherUtilities.segmentContainsLeftJoins(filter)) {
                return new OptionalJoinSegment<T>(filter);
            } else {
                return new JoinSegment<T>(filter);
            }
        } else if(node instanceof Join) {
            final Join join = (Join) node;
            if(MatcherUtilities.segmentContainsLeftJoins(join)) {
                return new OptionalJoinSegment<T>(join);
            } else {
                return new JoinSegment<T>(join);
            }
        } else if (node instanceof LeftJoin) {
            return new OptionalJoinSegment<T>((LeftJoin) node);
        } else {
            throw new IllegalArgumentException("Node must be a Join, Filter, or LeftJoin");
        }

    }

}
