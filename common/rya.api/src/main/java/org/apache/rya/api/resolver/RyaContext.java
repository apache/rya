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



import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rya.api.domain.RyaRange;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.impl.BooleanRyaTypeResolver;
import org.apache.rya.api.resolver.impl.ByteRyaTypeResolver;
import org.apache.rya.api.resolver.impl.CustomDatatypeResolver;
import org.apache.rya.api.resolver.impl.DateTimeRyaTypeResolver;
import org.apache.rya.api.resolver.impl.DoubleRyaTypeResolver;
import org.apache.rya.api.resolver.impl.FloatRyaTypeResolver;
import org.apache.rya.api.resolver.impl.IntegerRyaTypeResolver;
import org.apache.rya.api.resolver.impl.LongRyaTypeResolver;
import org.apache.rya.api.resolver.impl.RyaTypeResolverImpl;
import org.apache.rya.api.resolver.impl.RyaURIResolver;
import org.apache.rya.api.resolver.impl.ServiceBackedRyaTypeResolverMappings;
import org.apache.rya.api.resolver.impl.ShortRyaTypeResolver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.XMLSchema;

/**
 * Date: 7/16/12
 * Time: 12:04 PM
 */
public class RyaContext {

    public Log logger = LogFactory.getLog(RyaContext.class);

    private Map<URI, RyaTypeResolver> uriToResolver = new HashMap<URI, RyaTypeResolver>();
    private Map<Byte, RyaTypeResolver> byteToResolver = new HashMap<Byte, RyaTypeResolver>();
    private RyaTypeResolver defaultResolver = new CustomDatatypeResolver();

    private RyaContext() {
        //add default
        addDefaultMappings();
     }

    protected void addDefaultMappings() {
        if (logger.isDebugEnabled()) {
            logger.debug("Adding default mappings");
        }
        addRyaTypeResolverMapping(new RyaTypeResolverMapping(new RyaTypeResolverImpl())); // plain string
        addRyaTypeResolverMapping(new RyaTypeResolverMapping(new RyaURIResolver())); // uri
        addRyaTypeResolverMapping(new RyaTypeResolverMapping(new DateTimeRyaTypeResolver())); // dateTime
        addRyaTypeResolverMapping(new RyaTypeResolverMapping(new DoubleRyaTypeResolver())); // double
        addRyaTypeResolverMapping(new RyaTypeResolverMapping(new FloatRyaTypeResolver())); // float
        addRyaTypeResolverMapping(new RyaTypeResolverMapping(new IntegerRyaTypeResolver())); // integer
        addRyaTypeResolverMapping(new RyaTypeResolverMapping(new ShortRyaTypeResolver())); // short
        addRyaTypeResolverMapping(new RyaTypeResolverMapping(new LongRyaTypeResolver())); // long
        addRyaTypeResolverMapping(new RyaTypeResolverMapping(new BooleanRyaTypeResolver())); // boolean
        addRyaTypeResolverMapping(new RyaTypeResolverMapping(new ByteRyaTypeResolver())); // byte

        //int is integer
        uriToResolver.put(XMLSchema.INT, new IntegerRyaTypeResolver());

        //add service loaded mappings
        addRyaTypeResolverMappings(new ServiceBackedRyaTypeResolverMappings().getResolvers());
    }

    private static class RyaContextHolder {
        public static final RyaContext INSTANCE = new RyaContext();
     }

    public synchronized static RyaContext getInstance() {
        return RyaContextHolder.INSTANCE;
    }
    

    //need to go from datatype->resolver
    public RyaTypeResolver retrieveResolver(URI datatype) {
        RyaTypeResolver ryaTypeResolver = uriToResolver.get(datatype);
        if (ryaTypeResolver == null) return defaultResolver;
        return ryaTypeResolver;
    }

    //need to go from byte->resolver
    public RyaTypeResolver retrieveResolver(byte markerByte) {
        RyaTypeResolver ryaTypeResolver = byteToResolver.get(markerByte);
        if (ryaTypeResolver == null) return defaultResolver;
        return ryaTypeResolver;
    }

    public byte[] serialize(RyaType ryaType) throws RyaTypeResolverException {
        RyaTypeResolver ryaTypeResolver = retrieveResolver(ryaType.getDataType());
        if (ryaTypeResolver != null) {
            return ryaTypeResolver.serialize(ryaType);
        }
        return null;
    }

    public byte[][] serializeType(RyaType ryaType) throws RyaTypeResolverException {
        RyaTypeResolver ryaTypeResolver = retrieveResolver(ryaType.getDataType());
        if (ryaTypeResolver != null) {
            return ryaTypeResolver.serializeType(ryaType);
        }
        return null;
    }

    public RyaType deserialize(byte[] bytes) throws RyaTypeResolverException {
        RyaTypeResolver ryaTypeResolver = retrieveResolver(bytes[bytes.length - 1]);
        if (ryaTypeResolver != null) {
            return ryaTypeResolver.deserialize(bytes);
        }
        return null;
    }

    public void addRyaTypeResolverMapping(RyaTypeResolverMapping mapping) {
        if (!uriToResolver.containsKey(mapping.getRyaDataType())) {
            if (logger.isDebugEnabled()) {
                logger.debug("addRyaTypeResolverMapping uri:[" + mapping.getRyaDataType() + "] byte:[" + mapping.getMarkerByte() + "] for mapping[" + mapping + "]");
            }
            uriToResolver.put(mapping.getRyaDataType(), mapping.getRyaTypeResolver());
            byteToResolver.put(mapping.getMarkerByte(), mapping.getRyaTypeResolver());
        } else {
            logger.warn("Could not add ryaType mapping because one already exists. uri:[" + mapping.getRyaDataType() + "] byte:[" + mapping.getMarkerByte() + "] for mapping[" + mapping + "]");
        }
    }

    public void addRyaTypeResolverMappings(List<RyaTypeResolverMapping> mappings) {
        for (RyaTypeResolverMapping mapping : mappings) {
            addRyaTypeResolverMapping(mapping);
        }
    }

    public RyaTypeResolver removeRyaTypeResolver(URI dataType) {
        RyaTypeResolver ryaTypeResolver = uriToResolver.remove(dataType);
        if (ryaTypeResolver != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Removing ryaType Resolver uri[" + dataType + "] + [" + ryaTypeResolver + "]");
            }
            byteToResolver.remove(ryaTypeResolver.getMarkerByte());
            return ryaTypeResolver;
        }
        return null;
    }

    public RyaTypeResolver removeRyaTypeResolver(byte markerByte) {
        RyaTypeResolver ryaTypeResolver = byteToResolver.remove(markerByte);
        if (ryaTypeResolver != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Removing ryaType Resolver byte[" + markerByte + "] + [" + ryaTypeResolver + "]");
            }
            uriToResolver.remove(ryaTypeResolver.getRyaDataType());
            return ryaTypeResolver;
        }
        return null;
    }

    //transform range
    public RyaRange transformRange(RyaRange range) throws RyaTypeResolverException {
        RyaTypeResolver ryaTypeResolver = retrieveResolver(range.getStart().getDataType());
        if (ryaTypeResolver != null) {
            return ryaTypeResolver.transformRange(range);
        }
        return range;
    }

    public RyaTypeResolver getDefaultResolver() {
        return defaultResolver;
    }

    public void setDefaultResolver(RyaTypeResolver defaultResolver) {
        this.defaultResolver = defaultResolver;
    }
}
