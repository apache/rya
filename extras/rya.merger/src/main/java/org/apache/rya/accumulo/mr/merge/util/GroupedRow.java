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
package org.apache.rya.accumulo.mr.merge.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Composite {@link WritableComparable} consisting of an Accumulo row and a string representing a group, such that
 * records can be grouped by the group name and sorted based on the {@link Key} and {@link Value}. Natural ordering
 * (compareTo) compares group followed by key followed by value; SortComparator uses the natural ordering while
 * GroupComparator sorts only based on group.
 */
public class GroupedRow implements WritableComparable<GroupedRow> {
    private final Text group = new Text();
    private Key key = new Key();
    private Value value = new Value();

    /**
     * Set the group that this row belongs to.
     * @param name A common label
     */
    public void setGroup(final String name) {
        group.set(name);
    }

    /**
     * Set the Key
     * @param key Key associated with an Accumulo row
     */
    public void setKey(final Key key) {
        this.key = key;
    }

    /**
     * Set the Value
     * @param value Value associated with an Accumulo row
     */
    public void setValue(final Value value) {
        this.value = value;
    }

    /**
     * Get the group name
     * @return A label common to all rows that should be grouped together
     */
    public Text getGroup() {
        return group;
    }

    /**
     * Get the Key
     * @return The key portion of the row
     */
    public Key getKey() {
        return key;
    }

    /**
     * Get the Value
     * @return The value portion of the row
     */
    public Value getValue() {
        return value;
    }

    /**
     * Serialize the group, key, and value
     */
    @Override
    public void write(final DataOutput out) throws IOException {
        group.write(out);
        key.write(out);
        value.write(out);
    }

    /**
     * Deserialize the group, key, and value
     */
    @Override
    public void readFields(final DataInput in) throws IOException {
        group.readFields(in);
        key.readFields(in);
        value.readFields(in);
    }

    /**
     * Natural ordering; compares based on group and then key.
     */
    @Override
    public int compareTo(final GroupedRow o) {
        if (o == null) {
            return 1;
        }
        return new CompareToBuilder().append(group, o.group).append(key, o.key).append(value, o.value).toComparison();
    }

    /**
     * Generates a hash based on group, key, and value.
     */
    @Override
    public int hashCode() {
        return Objects.hash(group, key, value);
    }

    /**
     * Test equality (group, key, value).
     */
    @Override
    public boolean equals(final Object o) {
        if (o == this) { return true; }
        if (o != null && o instanceof GroupedRow) {
            final GroupedRow other = (GroupedRow) o;
            return new EqualsBuilder().append(group, other.group).append(key, other.key).append(value, other.value).isEquals();
        }
        return false;
    }

    /**
     * Comparator that sorts by group name, then by Key, then by Value.
     */
    public static class SortComparator extends WritableComparator {
        SortComparator() {
            super(GroupedRow.class, true);
        }
        /**
         * Compares the groups of two GroupedRow instances, and the keys if they share a group.
         */
        @Override
        public int compare(final WritableComparable wc1, final WritableComparable wc2) {
            final GroupedRow gk1 = (GroupedRow) wc1;
            final GroupedRow gk2 = (GroupedRow) wc2;
            return gk1.compareTo(gk2);
        }
    }

    /**
     * Comparator that only sorts by group, ignoring Key.
     */
    public static class GroupComparator extends WritableComparator {
        GroupComparator() {
            super(GroupedRow.class, true);
        }
        /**
         * Compares the groups of two GroupedRow instances.
         */
        @Override
        public int compare(final WritableComparable wc1, final WritableComparable wc2) {
            final GroupedRow gk1 = (GroupedRow) wc1;
            final GroupedRow gk2 = (GroupedRow) wc2;
            return gk1.group.compareTo(gk2.group);
        }
    }
}
