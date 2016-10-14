package org.apache.rya.api.resolver;

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



import org.apache.rya.api.domain.RyaRange;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaTypeRange;
import org.openrdf.model.URI;

/**
 * Date: 7/16/12
 * Time: 12:08 PM
 */
public interface RyaTypeResolver {
    public byte[] serialize(RyaType ryaType) throws RyaTypeResolverException;
    public byte[][] serializeType(RyaType ryaType) throws RyaTypeResolverException;

    public RyaType deserialize(byte[] bytes) throws RyaTypeResolverException;

    public RyaType newInstance();

    /**
     * @param bytes
     * @return true if this byte[] is deserializable by this resolver
     */
    public boolean deserializable(byte[] bytes);

    public URI getRyaDataType();

    byte getMarkerByte();

    /**
     * This will allow a resolver to modify a range. For example, a date time resolver, with a reverse index,
     * might want to reverse the start and stop
     *
     * @return
     * @throws RyaTypeResolverException
     */
    public RyaRange transformRange(RyaRange ryaRange) throws RyaTypeResolverException;

}
