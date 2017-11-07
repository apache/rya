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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rya.api.model.visibility;

/**
 * A Comparator for {@link WritableComparable}s.
 *
 * <p>
 * This base implemenation uses the natural ordering. To define alternate
 * orderings, override {@link #compare(WritableComparable,WritableComparable)}.
 *
 * <p>
 * One may optimize compare-intensive operations by overriding
 * {@link #compare(byte[],int,int,byte[],int,int)}. Static utility methods are
 * provided to assist in optimized implementations of this method.
 *
 * XXX
 * This class has been copied over because Rya has decided to use the Accumulo
 * implementation of visibilities to control who is able to access what data
 * within a Rya instance. Until we implement an Accumulo agnostic method for
 * handling those visibility expressions, we have chosen to pull the Accumulo
 * code into our API.
 *
 * Copied from accumulo's org.apache.hadoop.io.WritableComparator
 *   <dependancy>
 *     <groupId>org.apache.hadoop</groupId>
 *     <artifactId>hadoop-commons</artifactId>
 *     <version>2.5</version>
 *   </dependancy>
 */
public class WritableComparator {
    /** Lexicographic order of binary data. */
    public static int compareBytes(final byte[] b1, final int s1, final int l1, final byte[] b2, final int s2,
            final int l2) {
        return FastByteComparisons.compareTo(b1, s1, l1, b2, s2, l2);
    }
}