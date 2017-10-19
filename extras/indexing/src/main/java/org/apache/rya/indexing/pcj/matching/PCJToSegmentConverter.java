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
package org.apache.rya.indexing.pcj.matching;
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

import com.google.common.base.Preconditions;
import org.apache.rya.indexing.external.matching.ExternalSetConverter;
import org.apache.rya.indexing.external.matching.JoinSegment;
import org.apache.rya.indexing.external.matching.OptionalJoinSegment;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 * Implementation of {@link ExternalSetConverter} to convert {@link ExternalTupleSet}s
 * to {@link QuerySegment}s.
 *
 */
public class PCJToSegmentConverter implements ExternalSetConverter<ExternalTupleSet> {

    private static final PCJToOptionalJoinSegment optional = new PCJToOptionalJoinSegment();
    private static final PCJToJoinSegment join = new PCJToJoinSegment();


    @Override
    public QuerySegment<ExternalTupleSet> setToSegment(final ExternalTupleSet set) {
        Preconditions.checkNotNull(set);
        if (PCJOptimizerUtilities.tupleContainsLeftJoins(set.getTupleExpr())) {
            return optional.getSegment(set);
        } else {
            return join.getSegment(set);
        }
    }

    /**
     * This class extracts the {@link JoinSegment} from the {@link TupleExpr} of
     * specified PCJ.
     *
     */
    static class PCJToJoinSegment extends AbstractQueryModelVisitor<RuntimeException> {

        private JoinSegment<ExternalTupleSet> segment;

        private PCJToJoinSegment(){}

        public QuerySegment<ExternalTupleSet> getSegment(final ExternalTupleSet pcj) {
            segment = null;
            pcj.getTupleExpr().visit(this);
            return segment;
        }

        @Override
        public void meet(final Join join) {
            segment = new JoinSegment<ExternalTupleSet>(join);
        }

        @Override
        public void meet(final Filter filter) {
            segment = new JoinSegment<ExternalTupleSet>(filter);
        }

    }

    /**
     * This class extracts the {@link OptionalJoinSegment} of PCJ query.
     *
     */
    static class PCJToOptionalJoinSegment extends AbstractQueryModelVisitor<RuntimeException> {

        private OptionalJoinSegment<ExternalTupleSet> segment;

        private PCJToOptionalJoinSegment(){}

        public QuerySegment<ExternalTupleSet> getSegment(final ExternalTupleSet pcj) {
            segment = null;
            pcj.getTupleExpr().visit(this);
            return segment;
        }

        @Override
        public void meet(final Join join) {
            segment = new OptionalJoinSegment<ExternalTupleSet>(join);
        }

        @Override
        public void meet(final Filter filter) {
            segment = new OptionalJoinSegment<ExternalTupleSet>(filter);
        }

        @Override
        public void meet(final LeftJoin node) {
            segment = new OptionalJoinSegment<ExternalTupleSet>(node);
        }

    }

}
