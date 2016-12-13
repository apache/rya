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

import java.util.Collection;
import java.util.Iterator;

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

import java.util.List;
import java.util.Set;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

import com.google.common.base.Optional;

/**
 * This interface provides a framework for matching {@link ExternalSet}s to
 * subsets of a given {@link QuerySegment}.
 *
 */
public interface ExternalSetMatcher<T extends ExternalSet> {

    /**
     *
     *Matches specified ExternalSet to underlying QuerySegment and returns
     *true if match occurs and false otherwise.  Underlying QuerySegment is changed by
     *matching ExternalSet to matching query nodes.
     *
     * @param set
     *            - {@link ExternalSet} used to replace matching PCJ nodes when
     *            match occurs
     * @return - true is match and replace occurs and false otherwise
     */
    public boolean match(T set);
    
    /**
     *Matches specified ExternalSets to underlying QuerySegment and returns
     *the resulting matched QuerySegment.  Underlying QuerySegment is unchanged.
     * 
     * @param sets - Collection of ExternalSets to be matched to QuerySegment
     */
    public QuerySegment<T> match(Collection<T> sets);
    
    /**
     * Allows Matcher to iterate over different Collections of ExternalSets to find an optimal combination of matching ExternalSets.
     * Returns the QuerySegment whose node list has a minimum Rating according to either specified or default QueryNodeListRater.
     * 
     * @param sets - Iterator over Lists of ExternalSets to be matched to QuerySegment.
     * @param rater - Class that rates the resulting QuerySegment after each collection is matched for the purpose
     * of finding an optimal ExternalSet combination.  If the rater is not provided, the {@link BasicRater} is used.
     */
    public QuerySegment<T> match(Iterator<List<T>> sets, Optional<QueryNodeListRater> rater);

    /**
     * @return - TupleExpr constructed from {@link QuerySegment} with matched
     *         nodes
     */
    public TupleExpr getQuery();

    /**
     * Converts TupleExpr starting with given node to a {@link QuerySegment}.
     * Node must be a {@link Filter}, {@link Join}, or {@link LeftJoin}.
     * 
     * @param node
     * @return
     */
    public QuerySegment<T> nodeToQuerySegment(QueryModelNode node);

    /**
     * @return - all {@link TupleExpr} that haven't been matched to a PCJ
     */
    public Set<TupleExpr> getUnmatchedArgNodes();

    /**
     * Similar to {@link #getUnmatchedArgs()}, except does not ignore no arg
     * statements.
     * 
     * @return a list of all unmatched nodes.
     */
    public List<QueryModelNode> getAllUnmatchedNodes();

    /**
     *
     * @return - provided ordered view of QuerySegment nodes
     */
    public List<QueryModelNode> getOrderedNodes();

    /**
     *
     * @return - Set of {@link Filter}s of given QuerySegment
     */
    public Set<Filter> getFilters();

}
