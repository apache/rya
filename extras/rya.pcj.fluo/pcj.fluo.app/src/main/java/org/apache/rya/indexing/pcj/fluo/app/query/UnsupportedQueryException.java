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
package org.apache.rya.indexing.pcj.fluo.app.query;

import org.apache.rya.api.client.CreatePCJ.ExportStrategy;

/**
 * This Exception thrown if the Rya Fluo Application does not support
 * the given SPARQL query.  This could happen for a number of reasons. The
 * two most common reasons are that the query possesses some combination of query nodes
 * that the application can't evaluate, or that the {@link ExportStrategy} of the query
 * is incompatible with one of its query nodes.  
 *
 */
public class UnsupportedQueryException extends Exception {
    private static final long serialVersionUID = 1L;

    public UnsupportedQueryException(final String message) {
        super(message);
    }

    public UnsupportedQueryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
