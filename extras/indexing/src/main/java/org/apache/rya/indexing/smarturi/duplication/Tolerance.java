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
package org.apache.rya.indexing.smarturi.duplication;

import static java.util.Objects.requireNonNull;

import java.text.NumberFormat;

/**
 * The types of methods available to use for calculating tolerance.
 */
public class Tolerance {
    private final Double value;
    private final ToleranceType toleranceType;

    /**
     * Creates a new instance of {@link Tolerance}.
     * @param value the tolerance value. (not {@code null})
     * @param toleranceType the {@link ToleranceType}. (not {@code null})
     */
    public Tolerance(final Double value, final ToleranceType toleranceType) {
        this.value = requireNonNull(value);
        this.toleranceType = requireNonNull(toleranceType);
    }

    /**
     * @return the tolerance value.
     */
    public Double getValue() {
        return value;
    }

    /**
     * @return the {@link ToleranceType}.
     */
    public ToleranceType getToleranceType() {
        return toleranceType;
    }

    @Override
    public String toString() {
        switch (toleranceType) {
            case PERCENTAGE:
                return NumberFormat.getPercentInstance().format(value);
            case DIFFERENCE:
                return value.toString();
            default:
                return "Unknown Tolerance Type with value: " + value.toString();
        }
    }
}