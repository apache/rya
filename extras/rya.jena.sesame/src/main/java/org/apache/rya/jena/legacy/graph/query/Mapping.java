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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rya.jena.legacy.graph.query;

import java.util.Map;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.util.CollectionFactory;

/**
 * This class is used to record the mapping from [variable] Node's to
 * the indexes they are bound to in a Query. Nodes bound to negative values
 * are predeclared; the negative value is converted on index allocation.
 */
public class Mapping implements VariableIndexes {
    private final Map<Node, Integer> map;

    private int index = 0;
    private int preIndex = 0;

    /**
     * Create a new mapping in which all variables are unbound and the variables
     * of {@code preDeclare} will be allocated the first slots in the map in their
     * natural order. [This is so that the query domain elements that come out of the
     * matching process will be positioned to be suitable as query answers.]
     * @param preDeclare the array of {@link Node}s.
     */
    public Mapping(final Node[] preDeclare) {
        this.map = CollectionFactory.createHashedMap();
        index = preDeclare.length;
        for (final Node element : preDeclare) {
            preDeclare(element);
        }
    }

    private void preDeclare(final Node v) {
        map.put(v, new Integer(--preIndex));
    }

    /**
     * Get the index of a node in the mapping; undefined if the
     * node is not mapped.
     *
     * @param v the node to look up
     * @return the index of v in the mapping
     */
    public int indexOf(final Node v) {
        final int res = lookUp(v);
        if (res < 0) {
            throw new GraphQuery.UnboundVariableException(v);
        }
        return res;
    }

    @Override
    public int indexOf(final String name) {
        return indexOf(NodeFactory.createVariable(name));
    }

    /**
     * Get the index of a node in the mapping; return -1
     * if the node is not mapped.
     * @param v the node to look up
     * @return the index of v in the mapping
     */
    public int lookUp(final Node v) {
        final Integer i = map.get(v);
        if (i == null || i.intValue() < 0) {
            return -1;
        }
        return i.intValue();
    }

    /**
     * Allocate an index to the node {@code v}. {@code v} must not already be
     * mapped.
     *
     * @param v the node to be given an index
     * @return the value of the allocated index
     */
    public int newIndex(final Node v) {
        final Integer already = map.get(v);
        final int result = already == null ? index++ : -already.intValue() - 1;
        map.put(v, new Integer(result));
        return result;
    }

    /**
     * Answer the number of names currently held in the map.
     * @return the number of names in the map
     */
    public int size() {
        return map.size();
    }

    /**
     * Answer {@code true} if we have already bound {@code v} (pre-declaration doesn't
     * count).
     * @param v the {@link Node} to look up
     * @return {@code true} if this mapping has seen a binding occurrence of
     * {@code v}. {@code false} otherwise.
     */
    public boolean hasBound(final Node v) {
        return map.containsKey(v) && map.get(v).intValue() > -1;
    }

    /**
     * @return a string representing this mapping
     */
    @Override
    public String toString() {
        return map.toString();
    }
}