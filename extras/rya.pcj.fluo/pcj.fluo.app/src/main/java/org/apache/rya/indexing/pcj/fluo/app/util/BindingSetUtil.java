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
package org.apache.rya.indexing.pcj.fluo.app.util;

import static java.util.Objects.requireNonNull;

import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;

/**
 * A utility class that defines functions that make it easier to work with {@link BindingSet} objects.
 */
public class BindingSetUtil {

    /**
     * Create a new {@link BindingSet} that only includes the bindings whose names appear within the {@code variableOrder}.
     * If no binding is found for a variable, then that binding is just omitted from the resulting object.
     *
     * @param variableOrder - Defines which bindings will be kept. (not null)
     * @param bindingSet - Contains the source {@link Binding}s. (not null)
     * @return A new {@link BindingSet} containing only the specified bindings.
     */
    public static BindingSet keepBindings(final VariableOrder variableOrder, final BindingSet bindingSet) {
        requireNonNull(variableOrder);
        requireNonNull(bindingSet);

        final MapBindingSet result = new MapBindingSet();
        for(final String bindingName : variableOrder) {
            if(bindingSet.hasBinding(bindingName)) {
                final Binding binding = bindingSet.getBinding(bindingName);
                result.addBinding(binding);
            }
        }
        return result;
    }
}
