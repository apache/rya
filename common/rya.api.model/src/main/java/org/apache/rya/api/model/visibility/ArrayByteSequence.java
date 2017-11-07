/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rya.api.model.visibility;

import static com.google.common.base.Charsets.UTF_8;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * XXX
 * This class has been copied over because Rya has decided to use the Accumulo
 * implementation of visibilities to control who is able to access what data
 * within a Rya instance. Until we implement an Accumulo agnostic method for
 * handling those visibility expressions, we have chosen to pull the Accumulo
 * code into our API.
 *
 * Copied from accumulo's  org.apache.accumulo.core.data.ArrayByteSequence
 *   <dependancy>
 *     <groupId>org.apache.accumulo</groupId>
 *     <artifactId>accumulo-core</artifactId>
 *     <version>1.6.4</version>
 *   </dependancy>
 */
public class ArrayByteSequence extends ByteSequence implements Serializable {

    private static final long serialVersionUID = 1L;

    protected byte data[];
    protected int offset;
    protected int length;

    public ArrayByteSequence(final byte data[]) {
        this.data = data;
        offset = 0;
        length = data.length;
    }

    public ArrayByteSequence(final byte data[], final int offset, final int length) {

        if (offset < 0 || offset > data.length || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(" Bad offset and/or length data.length = " + data.length + " offset = "
                    + offset + " length = " + length);
        }

        this.data = data;
        this.offset = offset;
        this.length = length;

    }

    public ArrayByteSequence(final String s) {
        this(s.getBytes(UTF_8));
    }

    public ArrayByteSequence(final ByteBuffer buffer) {
        length = buffer.remaining();

        if (buffer.hasArray()) {
            data = buffer.array();
            offset = buffer.position();
        } else {
            data = new byte[length];
            offset = 0;
            buffer.get(data);
        }
    }

    @Override
    public byte byteAt(final int i) {

        if (i < 0) {
            throw new IllegalArgumentException("i < 0, " + i);
        }

        if (i >= length) {
            throw new IllegalArgumentException("i >= length, " + i + " >= " + length);
        }

        return data[offset + i];
    }

    @Override
    public byte[] getBackingArray() {
        return data;
    }

    @Override
    public boolean isBackedByArray() {
        return true;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public int offset() {
        return offset;
    }

    @Override
    public ByteSequence subSequence(final int start, final int end) {

        if (start > end || start < 0 || end > length) {
            throw new IllegalArgumentException(
                    "Bad start and/end start = " + start + " end=" + end + " offset=" + offset + " length=" + length);
        }

        return new ArrayByteSequence(data, offset + start, end - start);
    }

    @Override
    public byte[] toArray() {
        if (offset == 0 && length == data.length) {
            return data;
        }

        final byte[] copy = new byte[length];
        System.arraycopy(data, offset, copy, 0, length);
        return copy;
    }

    @Override
    public String toString() {
        return new String(data, offset, length, UTF_8);
    }
}