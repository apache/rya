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
package org.apache.rya.indexing.pcj.storage.accumulo;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;

/**
 * Converts {@link BindingSet}s into other representations. This library is
 * intended to convert between BindingSet and whatever format it is being
 * stored as. These formats are often optimized for query evaluation.
 *
 * @param <T> Defines the type of model {@link BindingSet}s will be converted into/from.
 */
@ParametersAreNonnullByDefault
public interface BindingSetConverter<T> {

   /**
    * Converts a {@link BindingSet} into the target model. The target model
    * may not include every {@link Binding} that was in the original BindingSet,
    * it may not include the binding names, and it may order the binding values.
    * All of this information is specified using a {@link VariableOrder}.
    * </p>
    * Because the resulting model may not include the binding names from the
    * original object, you must hold onto that information if you want to
    * convert the resulting model back into a BindingSet later. Because the
    * resulting model may only contain a subset of the original BindingSet's
    * bindings, some information may be lost, so you may not be able to convert
    * the target model back into the original BindingSet.
    *
    * @param bindingSet - The BindingSet that will be converted. (not null)
    * @param varOrder - Which bindings and in what order they will appear in the
    *   resulting model. (not null)
    * @return The BindingSet formatted as the target model.
    * @throws BindingSetConversionException The BindingSet was unable to be
    *   converted into the target model. This will happen if the BindingSet has
    *   a binding whose name is not in the VariableOrder or if one of the values
    *   could not be converted into the target model.
    */
   public T convert(BindingSet bindingSet, VariableOrder varOrder) throws BindingSetConversionException;

   /**
    * Converts the target model representation of a {@link BindingSet} as is
    * created by {@link #convert(BindingSet, VariableOrder)} back into a
    * BindingSet.
    * </p>
    * You must provide the Binding names and the order they were written to
    * by using a {@link VariableOrder}.
    * </p>
    * If there is no value for one of the variable order names, then that binding
    * will be missing from the resulting BindingSet.
    *
    * @param bindingSet - The BindingSet formatted as the target model that will
    *   be converted. (not null)
    * @param varOrder - The VariableOrder that was used to create the target model. (not null)
    * @return The {@link BindingSet} representation of the target model.
    * @throws BindingSetConversionException The target model was unable to be
    *   converted back into a BindingSet.
    */
   public BindingSet convert(T bindingSet, VariableOrder varOrder) throws BindingSetConversionException;

   /**
    * One of the conversion methods of {@link BindingSetConverter} was unable to
    * to convert the {@link BindingSet} to/from the converted model.
    */
   public static class BindingSetConversionException extends Exception {
       private static final long serialVersionUID = 1L;

       /**
        * Constructs an instance of {@link BindingSetConversionException}.
        *
        * @param message - Describes why this exception was thrown.
        */
       public BindingSetConversionException(final String message) {
           super(message);
       }

       /**
        * BindingSetConversionException
        *
        * @param message - Describes why this exception was thrown.
        * @param cause - The exception that caused this one to be thrown.
        */
       public BindingSetConversionException(final String message, final Throwable cause) {
           super(message, cause);
       }
   }
}