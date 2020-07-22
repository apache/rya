package org.apache.rya.api.query.strategy;

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

import java.io.IOException;
import java.util.Map;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaResource;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.resolver.triple.TripleRowRegex;

/**
 * Date: 7/14/12
 * Time: 7:21 AM
 */
public interface TriplePatternStrategy {

    ByteRange defineRange(RyaResource subject, RyaIRI predicate, RyaValue object, RyaResource context,
                                                          RdfCloudTripleStoreConfiguration conf) throws IOException;

    TABLE_LAYOUT getLayout();

    boolean handles(RyaResource subject, RyaIRI predicate, RyaValue object, RyaResource context);

    TripleRowRegex buildRegex(String subject, String predicate, String object, String context, byte[] objectTypeInfo);

}
