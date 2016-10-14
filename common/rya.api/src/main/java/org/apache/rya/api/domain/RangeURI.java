package org.apache.rya.api.domain;

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



import org.openrdf.model.URI;
import org.openrdf.model.Value;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/11/12
 * Time: 1:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class RangeURI extends RangeValue<URI> implements URI {

    public RangeURI(URI start, URI end) {
        super(start, end);
    }

    public RangeURI(RangeValue rangeValue) {
        super((URI) rangeValue.getStart(), (URI) rangeValue.getEnd());
    }

    @Override
    public String getNamespace() {
        throw new UnsupportedOperationException("Ranges do not have a namespace");
    }

    @Override
    public String getLocalName() {
        throw new UnsupportedOperationException("Ranges do not have a localname");
    }
}
