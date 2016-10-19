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
package org.apache.rya.indexing.pcj.fluo.app.export;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

import com.google.common.base.Optional;

/**
 * Contains common parsing functions that make it easier to interpret parameter maps.
 */
@DefaultAnnotation(NonNull.class)
public abstract class ParametersBase {

    protected final Map<String, String> params;

    /**
     * Constructs an instance of {@link ParametersBase}.
     *
     * @param params - The parameters that will be wrapped. (not null)
     */
    public ParametersBase(final Map<String, String> params) {
        this.params = checkNotNull(params);
    }

    protected static void setBoolean(final Map<String, String> params, final String paramName, final boolean value) {
        checkNotNull(params);
        checkNotNull(paramName);
        params.put(paramName, Boolean.toString(value));
    }

    protected static boolean getBoolean(final Map<String, String> params, final String paramName, final boolean defaultValue) {
        checkNotNull(params);
        checkNotNull(paramName);

        final Optional<String> paramValue = Optional.fromNullable( params.get(paramName) );
        return paramValue.isPresent() ? Boolean.parseBoolean( paramValue.get() ) : defaultValue;
    }
}