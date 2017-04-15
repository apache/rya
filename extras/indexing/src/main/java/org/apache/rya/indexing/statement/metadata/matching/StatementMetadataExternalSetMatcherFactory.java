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
import org.apache.rya.indexing.external.matching.AbstractExternalSetMatcherFactory;
import org.apache.rya.indexing.external.matching.ExternalSetMatcher;
import org.apache.rya.indexing.external.matching.JoinSegment;
import org.apache.rya.indexing.external.matching.JoinSegmentMatcher;
import org.apache.rya.indexing.external.matching.OptionalJoinSegment;
import org.apache.rya.indexing.external.matching.OptionalJoinSegmentMatcher;
import org.apache.rya.indexing.external.matching.QuerySegment;

/**
 * This class builds the appropriate {@link ExternalSetMatcher}s based on the
 * type of {@link QuerySegment} that is passed in.  
 *
 */
public class StatementMetadataExternalSetMatcherFactory
        extends AbstractExternalSetMatcherFactory<StatementMetadataNode<?>> {

    @Override
    protected ExternalSetMatcher<StatementMetadataNode<?>> getJoinSegmentMatcher(
            JoinSegment<StatementMetadataNode<?>> segment) {
        return new JoinSegmentMatcher<StatementMetadataNode<?>>(segment,
                new MetadataNodeToSegmentConverter());
    }

    @Override
    protected ExternalSetMatcher<StatementMetadataNode<?>> getOptionalJoinSegmentMatcher(
            OptionalJoinSegment<StatementMetadataNode<?>> segment) {
        return new OptionalJoinSegmentMatcher<StatementMetadataNode<?>>(segment,
                new MetadataNodeToSegmentConverter());
    }

}
