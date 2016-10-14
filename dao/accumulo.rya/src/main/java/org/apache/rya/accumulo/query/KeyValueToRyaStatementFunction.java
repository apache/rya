package org.apache.rya.accumulo.query;

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



import java.util.Map;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Function;

/**
 * Date: 1/30/13
 * Time: 2:09 PM
 */
public class KeyValueToRyaStatementFunction implements Function<Map.Entry<Key, Value>, RyaStatement> {

    private TABLE_LAYOUT tableLayout;
	private RyaTripleContext context;

    public KeyValueToRyaStatementFunction(TABLE_LAYOUT tableLayout, RyaTripleContext context) {
        this.tableLayout = tableLayout;
        this.context = context;
    }

    @Override
    public RyaStatement apply(Map.Entry<Key, Value> input) {
        Key key = input.getKey();
        Value value = input.getValue();
        RyaStatement statement = null;
        try {
            statement = context.deserializeTriple(tableLayout,
                    new TripleRow(key.getRowData().toArray(),
                            key.getColumnFamilyData().toArray(),
                            key.getColumnQualifierData().toArray(),
                            key.getTimestamp(),
                            key.getColumnVisibilityData().toArray(),
                            (value != null) ? value.get() : null
                    ));
        } catch (TripleRowResolverException e) {
            throw new RuntimeException(e);
        }

        return statement;
    }
}
