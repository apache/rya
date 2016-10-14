package org.apache.rya.api.resolver.impl;

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



import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.calrissian.mango.types.LexiTypeEncoders;
import org.calrissian.mango.types.TypeEncoder;
import org.calrissian.mango.types.exception.TypeDecodingException;
import org.calrissian.mango.types.exception.TypeEncodingException;
import org.openrdf.model.vocabulary.XMLSchema;

/**
 */
public class FloatRyaTypeResolver extends RyaTypeResolverImpl {
    public static final int FLOAT_LITERAL_MARKER = 11;
    public static final TypeEncoder<Float, String> FLOAT_TYPE_ENCODER = LexiTypeEncoders.floatEncoder();

    public FloatRyaTypeResolver() {
        super((byte) FLOAT_LITERAL_MARKER, XMLSchema.FLOAT);
    }

    @Override
    protected String serializeData(String data) throws RyaTypeResolverException {
        try {
            float value = Float.parseFloat(data);
            return FLOAT_TYPE_ENCODER.encode(value);
        } catch (NumberFormatException e) {
            throw new RyaTypeResolverException(
                    "Exception occurred serializing data[" + data + "]", e);
        } catch (TypeEncodingException e) {
            throw new RyaTypeResolverException(
                    "Exception occurred serializing data[" + data + "]", e);
        }
    }

    @Override
    protected String deserializeData(String value) throws RyaTypeResolverException {
        try {
            return FLOAT_TYPE_ENCODER.decode(value).toString();
        } catch (TypeDecodingException e) {
            throw new RyaTypeResolverException(
                    "Exception occurred deserializing data[" + value + "]", e);
        }
    }
}
