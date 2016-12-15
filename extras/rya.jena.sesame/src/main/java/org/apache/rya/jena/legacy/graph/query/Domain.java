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

import java.util.AbstractList;
import java.util.Arrays;

import org.apache.jena.graph.Node;

/**
 * A Domain is an answer to a Binding query. It satisfies the List
 * interface so that casual users don't have to worry about its special
 * features - for them, it is immutable (they only ever get to see Domains
 * that have emerged from the query process).
 */
public final class Domain extends AbstractList<Node> implements IndexValues {
    /**
     * The array holding the bound values.
     */
    private final Node[] value;

    /**
     * Initialize a Domain with a copy of a Node value array.
     * @param value the {@link Node} value array.
     */
    public Domain(final Node[] value) {
        final Node[] result = new Node[value.length];
        for (int i = 0; i < value.length; i += 1) {
            result[i] = value[i];
        }
        this.value = result;
    }

    /**
     * Initialize this Domain with {@code size} {@code null} slots.
     * @param size the amount of {@code null} slots.
     */
    public Domain(final int size) {
        this.value = new Node[size];
    }

    @Override
    public int size() {
        return value.length;
    }

    @Override
    public Node get(final int i) {
        return value[i];
    }

    /**
     * Sets the node element.
     * @param i the index.
     * @param node the {@link Node}.
     */
    public void setElement(final int i, final Node node) {
        value[i] = node;
    }

    /**
     * Gets the node element.
     * @param i the index.
     * @return the {@link Node}.
     */
    public Node getElement(final int i) {
        return value[i];
    }

    /**
     * Copies the {@link Domain}.
     * @return the copied {@link Domain}.
     */
    public Domain copy() {
        return new Domain(this.value);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof Domain && Arrays.equals(this.value, ((Domain) object).value) || super.equals(object);
    }

    @Override
    public String toString() {
        final StringBuffer b = new StringBuffer(200);
        b.append("<domain");
        for (int i = 0; i < value.length; i += 1) {
            b.append(" ").append(i).append(":").append(value[i]);
        }
        b.append(">");
        return b.toString();
    }
}