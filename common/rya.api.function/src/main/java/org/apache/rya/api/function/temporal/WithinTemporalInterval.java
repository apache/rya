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

package org.apache.rya.api.function.temporal;

import java.time.ZonedDateTime;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Filter function in a SPARQL query used to filter when a point of time is
 * within an interval.
 */
@DefaultAnnotation(NonNull.class)
public class WithinTemporalInterval extends TemporalIntervalRelationFunction {
    @Override
    public String getURI() {
        return TemporalURIs.WITHIN;
    }

    @Override
    protected boolean relation(final ZonedDateTime d1, final ZonedDateTime[] interval) {
        Objects.requireNonNull(d1);
        Objects.requireNonNull(interval);
        return d1.isAfter(interval[0]) && d1.isBefore(interval[1]);
    }
}
