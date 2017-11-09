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

import java.util.List;

import org.eclipse.rdf4j.query.algebra.QueryModelNode;

/**
 * Class used for determining an optimal query plan.  It assigns a score
 * between 0 and 1 to a list of QueryModelNodes.  A lower score indicates
 * that the List represents a better collection of nodes for building a query
 * plan.  Usually, the specified List is compared to a base List (original query),
 * and the specified List (mutated query) is compared to the original to determine
 * if there was any improvement in the query plan.  This is meant to be used in conjunction
 * with the method {@link ExternalSetMatcher#match(java.util.Iterator, com.google.common.base.Optional)}.
 *
 */
public interface QueryNodeListRater {

    public double rateQuerySegment(List<QueryModelNode> eNodes);

}
