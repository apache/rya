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
package org.apache.rya.api.function.aggregation;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The Sum and Count of the values that are being averaged. The average itself is derived from these values.
 */
@DefaultAnnotation(NonNull.class)
public class AverageState implements Serializable {
    private static final long serialVersionUID = 1L;

    private final BigDecimal sum;
    private final BigInteger count;

    /**
     * Constructs an instance of {@link AverageState} where the count and sum start at 0.
     */
    public AverageState() {
        sum = BigDecimal.ZERO;
        count = BigInteger.ZERO;
    }

    /**
     * Constructs an instance of {@link AverageState}.
     *
     * @param sum - The sum of the values that are averaged. (not null)
     * @param count - The number of values that are averaged. (not null)
     */
    public AverageState(final BigDecimal sum, final BigInteger count) {
        this.sum = requireNonNull(sum);
        this.count = requireNonNull(count);
    }

    /**
     * @return The sum of the values that are averaged.
     */
    public BigDecimal getSum() {
        return sum;
    }

    /**
     * @return The number of values that are averaged.
     */
    public BigInteger getCount() {
        return count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sum, count);
    }

    @Override
    public boolean equals(final Object o) {
        if(o instanceof AverageState) {
            final AverageState state = (AverageState) o;
            return Objects.equals(sum, state.sum) &&
                    Objects.equals(count, state.count);
        }
        return false;
    }

    @Override
    public String toString() {
        return "Sum: " + sum + " Count: " + count;
    }
}