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

import java.util.Optional;

import org.openrdf.query.algebra.AggregateOperator;
import org.openrdf.query.algebra.Avg;
import org.openrdf.query.algebra.Count;
import org.openrdf.query.algebra.Max;
import org.openrdf.query.algebra.Min;
import org.openrdf.query.algebra.Sum;

import com.google.common.collect.ImmutableMap;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The different types of Aggregation functions that an aggregate node may perform.
 */
@DefaultAnnotation(NonNull.class)
public enum AggregationType {
    MIN(Min.class),
    MAX(Max.class),
    COUNT(Count.class),
    SUM(Sum.class),
    AVERAGE(Avg.class);

    private final Class<? extends AggregateOperator> operatorClass;

    private AggregationType(final Class<? extends AggregateOperator> operatorClass) {
        this.operatorClass = requireNonNull(operatorClass);
    }

    private static final ImmutableMap<Class<? extends AggregateOperator>, AggregationType> byOperatorClass;
    static {
        final ImmutableMap.Builder<Class<? extends AggregateOperator>, AggregationType> builder = ImmutableMap.builder();
        for(final AggregationType type : AggregationType.values()) {
            builder.put(type.operatorClass, type);
        }
        byOperatorClass = builder.build();
    }

    public static Optional<AggregationType> byOperatorClass(final Class<? extends AggregateOperator> operatorClass) {
        return Optional.ofNullable( byOperatorClass.get(operatorClass) );
    }
}