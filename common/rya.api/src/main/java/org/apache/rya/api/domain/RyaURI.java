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
import org.openrdf.model.util.URIUtil;
import org.openrdf.model.vocabulary.XMLSchema;


/**
 * Date: 7/16/12
 * Time: 11:56 AM
 */
public class RyaURI extends RyaType {

    public RyaURI() {
        setDataType(XMLSchema.ANYURI);
    }

    public RyaURI(String data) {
        super(XMLSchema.ANYURI, data);
    }

    public RyaURI(String namespace, String data) {
        super(XMLSchema.ANYURI, namespace + data);
    }

    protected RyaURI(URI datatype, String data) {
        super(datatype, data);
    }

    @Override
    public void setData(String data) {
        super.setData(data);
        validate(data);
    }

    protected void validate(String data) {
        if (data == null)
            throw new IllegalArgumentException("Null not URI");
        URIUtil.getLocalNameIndex(data);
    }

}
