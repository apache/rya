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

import org.apache.rya.indexing.entity.query.EntityQueryNode;
import org.apache.rya.indexing.external.matching.AbstractExternalSetMatcherFactory;
import org.apache.rya.indexing.external.matching.ExternalSetMatcher;
import org.apache.rya.indexing.external.matching.JoinSegment;
import org.apache.rya.indexing.external.matching.JoinSegmentMatcher;
import org.apache.rya.indexing.external.matching.OptionalJoinSegment;
import org.apache.rya.indexing.external.matching.OptionalJoinSegmentMatcher;

/**
 * Factory used to build {@link EntityQueryNodeMatcher}s for the {@link EntityIndexOptimizer}.
 *
 */
public class EntityExternalSetMatcherFactory extends AbstractExternalSetMatcherFactory<EntityQueryNode> {

    @Override
    protected ExternalSetMatcher<EntityQueryNode> getJoinSegmentMatcher(final JoinSegment<EntityQueryNode> segment) {
        return new JoinSegmentMatcher<EntityQueryNode>(segment, new EntityToSegmentConverter());
    }

    @Override
    protected ExternalSetMatcher<EntityQueryNode> getOptionalJoinSegmentMatcher(final OptionalJoinSegment<EntityQueryNode> segment) {
        return new OptionalJoinSegmentMatcher<EntityQueryNode>(segment, new EntityToSegmentConverter());
    }
}
