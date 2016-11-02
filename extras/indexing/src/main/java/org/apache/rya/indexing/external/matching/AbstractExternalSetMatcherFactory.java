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
package org.apache.rya.indexing.external.matching;
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

import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

/**
 * This class takes in a given {@link Join}, {@Filter}, or {@link LeftJoin}
 * and provides the appropriate {@link ExternalSetMatcher} to match Entities to the
 * given query.
 */
public abstract class AbstractExternalSetMatcherFactory<T extends ExternalSet> {

    public ExternalSetMatcher<T> getMatcher(final QuerySegment<T> segment) {
        if(segment instanceof JoinSegment<?>) {
            return getJoinSegmentMatcher((JoinSegment<T>) segment);
        } else if(segment instanceof OptionalJoinSegment<?>) {
            return getOptionalJoinSegmentMatcher((OptionalJoinSegment<T>)segment);
        } else {
            throw new IllegalArgumentException("Invalid Segment.");
        }
    }

    protected abstract ExternalSetMatcher<T> getJoinSegmentMatcher(JoinSegment<T> segment);

    protected abstract ExternalSetMatcher<T> getOptionalJoinSegmentMatcher(OptionalJoinSegment<T> segment);

}
