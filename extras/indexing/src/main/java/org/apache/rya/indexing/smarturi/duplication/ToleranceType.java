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

import org.apache.commons.lang3.StringUtils;

/**
 * The types of methods available to use for calculating tolerance.
 */
public enum ToleranceType {
    /**
     * Indicates that the difference between two values must be within the
     * specified tolerance value to be accepted.
     */
    DIFFERENCE,
    /**
     * Indicates that the difference between two values divided by the original
     * value must fall within the specified tolerance percentage value to be
     * accepted.
     */
    PERCENTAGE;

    /**
     * Returns the {@link ToleranceType} that matches the specified name.
     * @param name the name to find.
     * @return the {@link ToleranceType} or {@code null} if none could be found.
     */
    public static ToleranceType getToleranceTypeByName(final String name) {
        if (StringUtils.isNotBlank(name)) {
            return ToleranceType.valueOf(name);
        }
        return null;
    }
}