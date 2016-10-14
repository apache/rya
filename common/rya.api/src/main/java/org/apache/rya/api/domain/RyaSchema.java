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



import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.openrdf.model.URI;

/**
 * Date: 7/16/12
 * Time: 11:59 AM
 */
public class RyaSchema {

    public static final String NAMESPACE = "urn:org.apache.rya/2012/05#";
    public static final String AUTH_NAMESPACE = "urn:org.apache.rya/auth/2012/05#";
    public static final String BNODE_NAMESPACE = "urn:org.apache.rya/bnode/2012/07#";

    //datatypes
    public static final URI NODE = RdfCloudTripleStoreConstants.VALUE_FACTORY.createURI(NAMESPACE, "node");
    public static final URI LANGUAGE = RdfCloudTripleStoreConstants.VALUE_FACTORY.createURI(NAMESPACE, "lang");

    //functions
    public static final URI RANGE = RdfCloudTripleStoreConstants.VALUE_FACTORY.createURI(NAMESPACE, "range");
}
