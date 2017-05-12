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
package org.apache.rya.indexing.pcj.storage.mongo;

import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter;
import org.bson.Document;
import org.openrdf.query.BindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Converts {@link BindingSet}s into other representations. This library is
 * intended to convert between BindingSet and {@link Document}.
 */
@DefaultAnnotation(NonNull.class)
public interface MongoBindingSetConverter extends BindingSetConverter<Document> {

    /**
     * Converts a {@link BindingSet} into a MongoDB model.
     *
     * @param bindingSet - The BindingSet that will be converted. (not null)
     * @return The BindingSet formatted as Mongo Bson object.
     * @throws BindingSetConversionException The BindingSet was unable to be
     *   converted. This will happen if one of the values could not be
     *   converted into the target model.
     */
    public Document convert(BindingSet bindingSet) throws BindingSetConversionException;

    /**
     * Converts a MongoDB model into a {@link BindingSet}.
     *
     * @param bindingSet - The bson that will be converted. (not null)
     * @return The BindingSet created from a Mongo Bson object.
     * @throws BindingSetConversionException The Bson was unable to be
     *   converted. This will happen if one of the values could not be
     *   converted into a BindingSet.
     */
    public BindingSet convert(Document bindingSet) throws BindingSetConversionException;
}
