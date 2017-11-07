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
package org.apache.rya.api.function.join;

import java.util.Iterator;

import org.apache.rya.api.model.VisibilityBindingSet;

/**
 * Defines each of the cases that may generate new join results when iteratively computing a query's join node.
 */
public interface IterativeJoin {

    /**
     * Invoked when a new {@link VisibilityBindingSet} is emitted from the left child
     * node of the join.
     *
     * @param newLeftResult - A new VisibilityBindingSet that has been emitted from the left child node.
     * @param rightResults - The right child node's binding sets that will be joined with the new left result. (not null)
     * @return The new BindingSet results for the join.
     */
    public Iterator<VisibilityBindingSet> newLeftResult(VisibilityBindingSet newLeftResult, Iterator<VisibilityBindingSet> rightResults);

    /**
     * Invoked when a new {@link VisibilityBindingSet} is emitted from the right child
     * node of the join.
     *
     * @param leftResults - The left child node's binding sets that will be joined with the new right result.
     * @param newRightResult - A new BindingSet that has been emitted from the right child node.
     * @return The new BindingSet results for the join.
     */
    public Iterator<VisibilityBindingSet> newRightResult(Iterator<VisibilityBindingSet> leftResults, VisibilityBindingSet newRightResult);
}
