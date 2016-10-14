package org.apache.rya.api.utils;

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



import com.google.common.base.Function;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.openrdf.query.BindingSet;

import java.util.Map;

/**
 * Date: 1/18/13
 * Time: 1:25 PM
 */
public class RyaStatementAddBindingSetFunction implements Function<RyaStatement, Map.Entry<RyaStatement, BindingSet>> {
    @Override
    public Map.Entry<RyaStatement, BindingSet> apply(RyaStatement ryaStatement) {
        return new RdfCloudTripleStoreUtils.CustomEntry<org.apache.rya.api.domain.RyaStatement, org.openrdf.query.BindingSet>(ryaStatement, null);
    }
}
