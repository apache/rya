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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.rya.api.domain.VarNameUtils;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * This implementation of the QueryNodeListRater assigns a score to a specified
 * list between and 0 and 1, where the lower the score, the better the list. It
 * is assumed that the specified list in
 * {@link BasicRater#rateQuerySegment(List)} is the result of matching
 * {@link ExternalSet}s to the original list specified in the constructor. The
 * method {@link BasicRater#rateQuerySegment(List)} determines a score based on
 * how much smaller the specified list is than the original, and based on how
 * many connected components the specified list has. Here the components are among
 * the graph obtained by drawing edges between QueryModelNodes with common
 * variables.
 *
 */
public class BasicRater implements QueryNodeListRater {

    private List<QueryModelNode> qNodes;

    public BasicRater(List<QueryModelNode> qNodes) {
        this.qNodes = qNodes;
    }

    @Override
    public double rateQuerySegment(List<QueryModelNode> eNodes) {
        return .6 * ((double) eNodes.size()) / qNodes.size() + .4 * getConnectedComponentRating(eNodes);
    }

    private double getConnectedComponentRating(List<QueryModelNode> eNodes) {

        Multimap<String, Integer> commonVarBin = HashMultimap.create();

        // bin QueryModelNode positions according to variable names
        for (int i = 0; i < eNodes.size(); i++) {
            QueryModelNode node = eNodes.get(i);
            if (node instanceof TupleExpr) {
                TupleExpr tup = (TupleExpr) node;
                Set<String> bindingNames = tup.getAssuredBindingNames();
                for (String name : bindingNames) {
                    if (!VarNameUtils.isConstant(name)) {
                        commonVarBin.put(name, i);
                    }
                }
            }
        }

        Set<List<Integer>> pairs = new HashSet<>();
        for (String var : commonVarBin.keySet()) {
            Set<Integer> pos = Sets.newHashSet(commonVarBin.get(var));
            pairs.addAll(Sets.cartesianProduct(pos, pos));
        }

        int numComponents = countComponents(eNodes.size(), pairs);
        return ((double) numComponents) / eNodes.size();

    }

    public int countComponents(int n, Set<List<Integer>> pairs) {
        int count = n;

        int[] root = new int[n];
        // initialize each node is an island
        for (int i = 0; i < n; i++) {
            root[i] = i;
        }

        for (List<Integer> pair : pairs) {

            int x = pair.get(0);
            int y = pair.get(1);

            // ignore self directed edges
            if (x == y) {
                continue;
            }

            int xRoot = getRoot(root, x);
            int yRoot = getRoot(root, y);

            if (xRoot != yRoot) {
                count--;
                root[xRoot] = yRoot;
            }

        }

        return count;
    }

    public int getRoot(int[] arr, int i) {
        while (arr[i] != i) {
            arr[i] = arr[arr[i]];
            i = arr[i];
        }
        return i;
    }

}
