package org.apache.rya.reasoning.mr;

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Allows us to use a URI or bnode for a key.
 */
public class ResourceWritable implements WritableComparable<ResourceWritable> {
    private Resource val;
    private int key = 0; // Allows for secondary sort

    public Resource get() {
        return val;
    }

    public void set(Resource val) {
        this.val = val;
    }

    public void set(Resource val, int sortKey) {
        this.val = val;
        this.key = sortKey;
    }

    public void setSortKey(int key) {
        this.key = key;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (val == null) {
            out.writeUTF("");
        }
        else {
            out.writeUTF(val.toString());
        }
        out.writeInt(key);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String s = in.readUTF();
        if (s.length() > 0) {
            if (s.startsWith("_")) {
                val = ValueFactoryImpl.getInstance().createBNode(s.substring(2));
            }
            else {
                val = ValueFactoryImpl.getInstance().createURI(s);
            }
        }
        key = in.readInt();
    }

    @Override
    public int compareTo(ResourceWritable other) {
        return val.stringValue().compareTo(other.val.stringValue());
    }

    @Override
    public String toString() {
        return "<" + val.stringValue() + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        else if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        ResourceWritable other = (ResourceWritable) o;
        if (this.val == null) {
            return other.val == null;
        }
        else if (other.val == null) {
            return false;
        }
        else if (this.val.stringValue() == null) {
            return other.val.stringValue() == null;
        }
        else {
            return this.val.stringValue().equals(other.val.stringValue());
        }
    }

    @Override
    public int hashCode() {
        return val != null ? val.stringValue().hashCode() : 0;
    }

    public static class PrimaryComparator extends WritableComparator {
        PrimaryComparator() {
            super(ResourceWritable.class, true);
        }
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            ResourceWritable node1 = (ResourceWritable) wc1;
            ResourceWritable node2 = (ResourceWritable) wc2;
            return node1.compareTo(node2);
        }
    }

    public static class SecondaryComparator extends WritableComparator {
        SecondaryComparator() {
            super(ResourceWritable.class, true);
        }
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            ResourceWritable node1 = (ResourceWritable) wc1;
            ResourceWritable node2 = (ResourceWritable) wc2;
            int result = node1.compareTo(node2);
            if (result == 0) {
                result = node1.key - node2.key;
            }
            return result;
        }
    }
}
