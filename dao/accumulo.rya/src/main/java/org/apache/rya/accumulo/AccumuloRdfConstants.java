package org.apache.rya.accumulo;

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



import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Interface AccumuloRdfConstants
 * Date: Mar 1, 2012
 * Time: 7:24:52 PM
 */
public interface AccumuloRdfConstants {
    public static final Authorizations ALL_AUTHORIZATIONS = Constants.NO_AUTHS;

    public static final Value EMPTY_VALUE = new Value(new byte[0]);

    public static final ColumnVisibility EMPTY_CV = new ColumnVisibility(new byte[0]);
}
