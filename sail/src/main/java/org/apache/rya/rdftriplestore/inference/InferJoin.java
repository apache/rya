package org.apache.rya.rdftriplestore.inference;

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



import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;

import java.util.HashMap;
import java.util.Map;

/**
 * Class InferJoin
 * Date: Apr 16, 2011
 * Time: 7:29:40 AM
 */
public class InferJoin extends Join {

    private Map<String, String> properties = new HashMap<String, String>();

    public InferJoin() {
    }

    public InferJoin(TupleExpr leftArg, TupleExpr rightArg) {
        super(leftArg, rightArg);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

}
