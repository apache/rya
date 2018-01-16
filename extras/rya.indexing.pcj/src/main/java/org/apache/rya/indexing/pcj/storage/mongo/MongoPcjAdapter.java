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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

/**
 * Converts a Pcj for storage in mongoDB or retrieval from mongoDB.
 */
public class MongoPcjAdapter implements BindingSetConverter<Bson> {
    @Override
    public Bson convert(final BindingSet bindingSet, final VariableOrder varOrder) throws BindingSetConversionException {
        checkNotNull(bindingSet);
        checkNotNull(varOrder);

        final Document bson = new Document();
        for(final String varName: varOrder) {
            // Only write information for a variable name if the binding set contains it.
            if(bindingSet.hasBinding(varName)) {
                final RyaType rt = RdfToRyaConversions.convertValue(bindingSet.getBinding(varName).getValue());
                final BsonArray rtAsBson = new BsonArray();
                rtAsBson.add(new BsonString(rt.getDataType().toString()));
                rtAsBson.add(new BsonString(rt.getData()));
                bson.append(varName, rtAsBson);
            }
        }

        return bson;
    }

    @Override
    public BindingSet convert(final Bson bindingSetBson, final VariableOrder varOrder) throws BindingSetConversionException {
        checkNotNull(bindingSetBson);
        checkNotNull(varOrder);

        final CodecRegistry registry = CodecRegistries.fromCodecs(new DocumentCodec());
        final BsonDocument doc = bindingSetBson.toBsonDocument(Document.class, registry);
        final String[] varOrderStrings = varOrder.toArray();
        final QueryBindingSet bindingSet = new QueryBindingSet();

        for(final String var : varOrderStrings) {
            if(doc.containsKey(var)) {
                final BsonValue binding = doc.get(var);
                final RyaType rt = new RyaType(
                        new URIImpl(binding.asArray().get(0).asString().getValue()),
                        binding.asArray().get(1).asString().getValue());
                bindingSet.addBinding(var, RyaToRdfConversions.convertValue(rt));
            }
        }

        return bindingSet;
    }
}
